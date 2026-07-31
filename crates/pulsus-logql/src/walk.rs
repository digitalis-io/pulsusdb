//! Iterative walkers over the recursive LogQL types (issue #272).
//!
//! # Why this module exists
//!
//! Every recursive walk over a LogQL AST or plan type — `Debug`, `Clone`,
//! `PartialEq`, `Hash`, `Display`, drop glue, and the hand-written
//! planner/compiler walks — costs one machine-stack frame per level. A
//! flat `a or b or c …` chain parses at depth 1 into a **left-deep** tree,
//! so query *width* becomes tree *depth* and a single request can abort
//! the process (`fatal runtime error: stack overflow`, exit 134). A depth
//! guard cannot bound that: parenthesis nesting contributes zero compiled
//! depth while term count contributes all of it.
//!
//! This module moves the recursion onto the heap. Every SCC-internal child
//! slot is spelled [`Child`]/[`ChildVec`], which implement **nothing** —
//! no `Debug`, `Display`, `Clone`, `PartialEq`, `Eq`, `Hash`, `Default`,
//! and deliberately no `Deref`. They expose their contents only as **inert
//! handles** ([`Ref`], [`RefSlice`], [`Slot`], [`SlotVec`]), which can be
//! opened only by presenting a [`Walk`] — a zero-sized capability whose
//! single constructor is private to this file and which no public function
//! ever yields. The consequences are compile errors, not conventions:
//!
//! * `#[derive(Debug, Clone, PartialEq, Hash)]` on a type holding a slot
//!   fails to compile (C1).
//! * `write!(f, "{child}")`, `child.clone()`, `child == other` and
//!   `child.hash(state)` inside a hand-written impl fail to compile (C1).
//! * A new variant cannot be silently un-walked: every driver and every
//!   [`Scc`] method is an exhaustive `match` with no `_` arm (C2).
//! * Once a manual impl exists the matching `#[derive]` is a conflicting
//!   implementation (C3).
//!
//! What this does **not** prove: a consumer holding a node a driver
//! yielded may deliberately call a helper that re-enters a driver, one
//! frame per node. That residue is covered behaviourally by the paired
//! pinned-stack gates, never by a claim here.
//!
//! # The child edge is the single source of truth
//!
//! [`Scc::child`] is the only child relation. [`arity`], [`sole_child`]
//! and [`push_children`] are **free functions** derived from it, so no
//! `impl Scc` can make the spine route and the stack route disagree — the
//! divergence does not exist rather than being tested for.
//!
//! # Allocation
//!
//! Spine descent ([`descend_spine`]) and the in-loop sole-child descent
//! inside [`preorder`]/[`find_preorder`] hold one loop variable and touch
//! no heap at all. Branching walks use [`ChunkStack`], whose element
//! chunks are allocated one at a time and never reallocated, and whose
//! per-push cost is knowable **before** the push through
//! [`ChunkStack::next_push_bytes`] — in release as well as debug. No
//! invariant in this module is enforced by a `debug_assert`.

use std::fmt;
use std::fmt::Write as _;
use std::mem;
use std::ops::ControlFlow;

// ---------------------------------------------------------------------
// The capability
// ---------------------------------------------------------------------

/// Proof that the holder is executing inside a `walk.rs` driver loop.
///
/// The tuple field is private and [`Walk::new`] is the **only**
/// constructor, private to this module. A `Walk` is never returned by a
/// public function and never passed to a caller closure: the only
/// operations that consume one are [`Scc::open`] and [`Scc::open_slot`]
/// (plus the four handle openers below), which drivers call and which do
/// nothing but map a handle kind to a reference kind.
pub struct Walk(());

impl Walk {
    /// The single constructor. Private by design — see the type doc.
    #[inline]
    fn new() -> Self {
        Walk(())
    }
}

impl fmt::Debug for Walk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Walk")
    }
}

// ---------------------------------------------------------------------
// Inert handles
// ---------------------------------------------------------------------

/// An inert handle to a shared reference. Implements nothing beyond
/// `Copy`/`Clone` (copying a handle traverses nothing): no `Debug`,
/// `Display`, `PartialEq`, `Hash`, `Deref`.
pub struct Ref<'a, T>(&'a T);

impl<T> Clone for Ref<'_, T> {
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}
impl<T> Copy for Ref<'_, T> {}

impl<'a, T> Ref<'a, T> {
    /// Wrapping is inert and free.
    #[inline]
    pub fn new(v: &'a T) -> Self {
        Ref(v)
    }

    /// Capability-gated: opening requires a driver's [`Walk`].
    #[inline]
    pub fn open(self, _: &Walk) -> &'a T {
        self.0
    }
}

/// An inert handle to a shared slice of children.
pub struct RefSlice<'a, T>(&'a [T]);

impl<T> Clone for RefSlice<'_, T> {
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}
impl<T> Copy for RefSlice<'_, T> {}

impl<'a, T> RefSlice<'a, T> {
    #[inline]
    pub fn new(v: &'a [T]) -> Self {
        RefSlice(v)
    }

    #[inline]
    pub fn len(self) -> usize {
        self.0.len()
    }

    #[inline]
    pub fn is_empty(self) -> bool {
        self.0.is_empty()
    }

    /// Handle -> handle. Inert: no token, no allocation, no dispatch.
    #[inline]
    pub fn get(self, i: usize) -> Option<Ref<'a, T>> {
        self.0.get(i).map(Ref::new)
    }

    #[inline]
    pub fn open(self, _: &Walk) -> &'a [T] {
        self.0
    }
}

/// An inert handle to a mutable reference.
pub struct Slot<'a, T>(&'a mut T);

impl<'a, T> Slot<'a, T> {
    /// Wrapping is inert and free — the mirror of [`Ref::new`]. Needed
    /// because an `impl Drop` holds `&mut self` and must build the SCC's
    /// `Slot` kind from it.
    #[inline]
    pub fn new(v: &'a mut T) -> Self {
        Slot(v)
    }

    #[inline]
    pub fn open(self, _: &Walk) -> &'a mut T {
        self.0
    }
}

/// An inert handle to a mutable child vector.
pub struct SlotVec<'a, T>(&'a mut Vec<T>);

impl<'a, T> SlotVec<'a, T> {
    #[inline]
    pub fn new(v: &'a mut Vec<T>) -> Self {
        SlotVec(v)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[inline]
    pub fn open(self, _: &Walk) -> &'a mut Vec<T> {
        self.0
    }
}

// ---------------------------------------------------------------------
// The slot newtypes: the ONLY spelling of an SCC-internal child
// ---------------------------------------------------------------------

/// One SCC-internal child. A newtype over `Box<T>` with **no** derivable
/// trait and **no** `Deref` — a `Deref` would re-open `child.clone()` /
/// `child.hash(..)` by auto-deref, which is the whole point of the type.
///
/// There is deliberately no `get`, `get_mut` or `into_inner`: every
/// consumer goes through a driver or a wrapper.
pub struct Child<T>(Box<T>);

impl<T> Child<T> {
    #[inline]
    pub fn new(v: T) -> Self {
        Child(Box::new(v))
    }

    /// An inert handle to the child. Opening it needs a [`Walk`].
    #[inline]
    pub fn peek(&self) -> Ref<'_, T> {
        Ref::new(&self.0)
    }

    /// An inert mutable handle to the child. Opening it needs a [`Walk`].
    #[inline]
    pub fn slot(&mut self) -> Slot<'_, T> {
        Slot::new(&mut self.0)
    }

    /// Swaps the child for `v` **in place**, returning the previous value.
    ///
    /// Token-free by necessity: `Scc::steal_children` runs inside a
    /// `Drop`, which cannot mint a capability, and the replacement must
    /// reuse the existing `Box` (a `Box::new(placeholder())` would
    /// allocate once per node inside a `Drop`).
    ///
    /// This does **not** re-open the edge class `Child` exists to close:
    /// it needs `&mut` on the parent, so none of the shared-reference
    /// walks (`Debug`, `Display`, `Clone`, `PartialEq`, `Hash`, and every
    /// `#[derive]`) can reach it, and it yields an owned value rather
    /// than a child reference.
    #[inline]
    pub fn replace(&mut self, v: T) -> T {
        mem::replace(&mut self.0, v)
    }
}

/// N SCC-internal children. A newtype over `Vec<T>` with the same
/// restrictions as [`Child`].
pub struct ChildVec<T>(Vec<T>);

impl<T> ChildVec<T> {
    #[inline]
    pub fn new(v: Vec<T>) -> Self {
        ChildVec(v)
    }

    #[inline]
    pub fn peek(&self) -> RefSlice<'_, T> {
        RefSlice::new(&self.0)
    }

    #[inline]
    pub fn slot(&mut self) -> SlotVec<'_, T> {
        SlotVec::new(&mut self.0)
    }

    /// Takes the children, leaving an empty (non-allocating) vector.
    /// Same disposition as [`Child::replace`].
    #[inline]
    pub fn take(&mut self) -> Vec<T> {
        mem::take(&mut self.0)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

// ---------------------------------------------------------------------
// The segmented stack
// ---------------------------------------------------------------------

/// Elements per element chunk. Chunks are allocated one at a time and
/// never reallocated, so no two element buffers are ever live.
pub const WALK_CHUNK_ITEMS: usize = 256;

/// The chunk index's first capacity; it doubles from there.
pub const INDEX_INIT: usize = 4;

/// Per-allocation envelope, anchored on `RETAINED_ENTRY_OVERHEAD`
/// (`crates/pulsus-read/src/traces/exec.rs`).
pub const WALK_ALLOC_ENVELOPE_BYTES: usize = 64;

/// Conservatism applied to a chosen index capacity, so the charge stays
/// an upper bound in RELEASE without any runtime check.
pub const INDEX_CHARGE_FACTOR: usize = 2;

/// A segmented stack.
///
/// Element chunks are allocated one at a time and **never** reallocated,
/// so no two element buffers are ever live; chunks are retained until
/// drop, so the charge is monotone to the peak and a length oscillating
/// across a chunk boundary cannot thrash.
pub struct ChunkStack<T> {
    chunks: Vec<Vec<T>>,
    len: usize,
}

impl<T> fmt::Debug for ChunkStack<T> {
    /// Deliberately not `T: Debug`-bounded: a `ChunkStack` holds inert
    /// handles and owned SCC values, neither of which may be formatted
    /// per element without re-opening the edge class `Child` closes.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChunkStack")
            .field("len", &self.len)
            .field("chunks", &self.chunks.len())
            .field("index_capacity", &self.chunks.capacity())
            .finish()
    }
}

impl<T> Default for ChunkStack<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> ChunkStack<T> {
    #[inline]
    pub const fn new() -> Self {
        ChunkStack {
            chunks: Vec::new(),
            len: 0,
        }
    }

    /// The capacity this type will *choose* for the index on its next
    /// growth: `0 -> INDEX_INIT`, else `cap -> 2*cap`.
    #[inline]
    fn next_index_cap(&self) -> usize {
        let cap = self.chunks.capacity();
        if cap == 0 { INDEX_INIT } else { 2 * cap }
    }

    #[inline]
    fn needs_chunk(&self) -> bool {
        self.len == self.chunks.len() * WALK_CHUNK_ITEMS
    }

    #[inline]
    fn index_grows(&self) -> bool {
        self.chunks.len() == self.chunks.capacity()
    }

    /// A **conservative upper bound** (not an exact figure) on the bytes
    /// the next [`push`](Self::push) will request from the global
    /// allocator, computed entirely from values known before the call —
    /// in release as well as debug. No `debug_assert` carries any part of
    /// this bound.
    ///
    /// ```text
    ///   (needs a chunk ? WALK_CHUNK_ITEMS * size_of::<T>() + ENVELOPE : 0)
    /// + (index grows  ? INDEX_CHARGE_FACTOR * next_index_cap()
    ///                     * size_of::<Vec<T>>() + ENVELOPE            : 0)
    /// ```
    ///
    /// `next_index_cap()` is chosen by this type and requested with
    /// `reserve_exact`, so the request is ours. `Vec` may round a request
    /// up; charging `2 x new_cap` where the true transient is
    /// `old + new = 1.5 x new_cap` leaves >= 33 % headroom on the new
    /// buffer, which is why no capacity equality has to be checked at run
    /// time.
    #[inline]
    pub fn next_push_bytes(&self) -> usize {
        if !self.needs_chunk() {
            return 0;
        }
        let mut bytes = WALK_CHUNK_ITEMS * mem::size_of::<T>() + WALK_ALLOC_ENVELOPE_BYTES;
        if self.index_grows() {
            bytes += INDEX_CHARGE_FACTOR * self.next_index_cap() * mem::size_of::<Vec<T>>()
                + WALK_ALLOC_ENVELOPE_BYTES;
        }
        bytes
    }

    #[inline]
    pub fn push(&mut self, v: T) {
        if self.needs_chunk() {
            if self.index_grows() {
                let want = self.next_index_cap();
                self.chunks.reserve_exact(want - self.chunks.len());
            }
            self.chunks.push(Vec::with_capacity(WALK_CHUNK_ITEMS));
        }
        let chunk = self.len / WALK_CHUNK_ITEMS;
        self.chunks[chunk].push(v);
        self.len += 1;
    }

    #[inline]
    pub fn pop(&mut self) -> Option<T> {
        if self.len == 0 {
            return None;
        }
        self.len -= 1;
        let chunk = self.len / WALK_CHUNK_ITEMS;
        self.chunks[chunk].pop()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Number of element chunks currently allocated. Test/diagnostic
    /// surface for the byte gates; never used to carry an invariant.
    #[inline]
    pub fn chunk_count(&self) -> usize {
        self.chunks.len()
    }

    /// The chunk index's current capacity. Same disposition as
    /// [`chunk_count`](Self::chunk_count).
    #[inline]
    pub fn index_capacity(&self) -> usize {
        self.chunks.capacity()
    }
}

// ---------------------------------------------------------------------
// The heterogeneous SCC algebra
// ---------------------------------------------------------------------

/// One strongly-connected component of the "contains" graph, walked as a
/// single node algebra.
///
/// The SCC is allowed to be **heterogeneous**: `MetricExpr` reaches
/// `VariantsExpr` through a `Child`, and `VariantsExpr` reaches
/// `MetricExpr` through a `ChildVec`, so the kinds are two-variant enums
/// rather than a self type.
///
/// **Exactly two methods take a `&Walk`** ([`open`](Scc::open) and
/// [`open_slot`](Scc::open_slot)), each a pure handle -> reference kind
/// map with no dispatch in the body. Every other method takes an already
/// opened node, so no token is needed to implement or to call them, and a
/// consumer holding an opened node still cannot open a child handle.
///
/// This trait carries **no** `#[cfg(test)]` item: its surface is identical
/// in every build mode and in every dependent crate. (A `cfg`-conditional
/// trait surface is not a trait surface — `E0407`.)
pub trait Scc: Sized + 'static {
    /// Inert handle kinds.
    type Ref<'a>: Copy;
    /// Plain-reference kinds — what driver callbacks see.
    type Node<'a>: Copy;
    /// Inert mutable handle kinds.
    type Slot<'a>;
    /// Plain `&mut` kinds.
    type SlotNode<'a>;
    /// Owned kinds.
    type Val;

    /// Inert: wrapping a reference traverses nothing.
    fn wrap<'a>(n: Self::Node<'a>) -> Self::Ref<'a>
    where
        Self: 'a;

    /// TOKEN — a pure kind map, no dispatch.
    fn open<'a>(r: Self::Ref<'a>, w: &Walk) -> Self::Node<'a>
    where
        Self: 'a;

    /// TOKEN — a pure kind map, no dispatch.
    fn open_slot<'a>(s: Self::Slot<'a>, w: &Walk) -> Self::SlotNode<'a>
    where
        Self: 'a;

    /// Reborrows an opened mutable node as the shared `Node` kind.
    ///
    /// Inert: no token, no dispatch, no allocation — a pure kind map, the
    /// mirror of [`wrap`](Scc::wrap) on the mutable side. It exists
    /// because [`dismantle`]'s emptied-node fast path must inspect the
    /// child edges of a node it holds mutably, and an opaque `&mut`
    /// associated kind cannot otherwise be read as a shared one.
    fn slot_node_ref<'x>(s: &'x Self::SlotNode<'_>) -> Self::Node<'x>
    where
        Self: 'x;

    /// THE canonical child edge and the single source of truth for this
    /// SCC's containment graph.
    ///
    /// TOTAL: `Some` iff `i < arity(n)`; children are indexed
    /// LEFT-TO-RIGHT in declaration order. Exhaustive `match`, no `_` arm
    /// (C2). Yields an INERT handle, takes NO token, allocates nothing.
    ///
    /// Every arm must be a literal index ladder or a single `slice.get(i)`
    /// so the relation is **prefix-closed** by construction.
    fn child<'a>(n: Self::Node<'a>, i: usize) -> Option<Self::Ref<'a>>
    where
        Self: 'a;

    /// Compares every own field, the child arity and the node kind.
    /// Never descends.
    fn shallow_eq(a: Self::Node<'_>, b: Self::Node<'_>) -> bool;

    /// Clones the node's own fields, draining EXACTLY `arity(n)` values
    /// off the tail of `kids` (which are its children, left-to-right).
    /// A kind mismatch is unreachable by construction and is
    /// `unreachable!()` — the clone path is never a `Drop` path, so a
    /// release panic there is safe and is the correct failure.
    fn rebuild(n: Self::Node<'_>, kids: &mut Vec<Self::Val>) -> Self::Val;

    /// Releases the node's own (non-child) heap in place.
    ///
    /// Takes `&mut Self::SlotNode<'_>` rather than the by-value form: a
    /// `SlotNode` is a `&mut` kind and is therefore not `Copy`, so
    /// `dismantle` could not call this and
    /// [`steal_children`](Scc::steal_children) on the same node otherwise.
    fn take_own_fields(s: &mut Self::SlotNode<'_>);

    /// Replaces each child with a SAME-KIND, non-allocating placeholder
    /// and pushes the taken values RIGHT-TO-LEFT, so LIFO pop order is
    /// left-to-right.
    ///
    /// Single slots are taken by `mem::replace` on the **contents of the
    /// existing box** — never `Box::new(placeholder())`, which would
    /// allocate once per node inside a `Drop`.
    fn steal_children(s: &mut Self::SlotNode<'_>, out: &mut ChunkStack<Self::Val>);

    /// Inert: an owned value's handle.
    fn val_ref(v: &Self::Val) -> Self::Ref<'_>;

    /// Inert: an owned value's mutable handle.
    fn val_slot(v: &mut Self::Val) -> Self::Slot<'_>;
}

// ---------------------------------------------------------------------
// Free functions derived from `child`
// ---------------------------------------------------------------------

/// DERIVED from [`Scc::child`]. Not a trait method: no `impl Scc` can
/// supply a count that disagrees with the edge relation.
#[inline]
pub fn arity<S: Scc>(n: S::Node<'_>) -> usize {
    let mut i = 0;
    while S::child(n, i).is_some() {
        i += 1;
    }
    i
}

/// DERIVED from [`Scc::child`], O(1) — never calls [`arity`], so a wide
/// N-ary node does not pay an O(N) probe on the spine path.
#[inline]
pub fn sole_child<'a, S: Scc>(n: S::Node<'a>) -> Option<S::Ref<'a>> {
    if S::child(n, 1).is_some() {
        None
    } else {
        S::child(n, 0)
    }
}

/// DERIVED from [`Scc::child`]; pushes RIGHT-TO-LEFT so LIFO pop order is
/// left-to-right.
#[inline]
pub fn push_children<'a, S: Scc>(n: S::Node<'a>, out: &mut ChunkStack<S::Ref<'a>>) {
    for i in (0..arity::<S>(n)).rev() {
        let Some(r) = S::child(n, i) else {
            unreachable!("child(i) is Some for every i < arity(n) — `child` is prefix-closed")
        };
        out.push(r);
    }
}

// ---------------------------------------------------------------------
// Drivers
// ---------------------------------------------------------------------

/// Where a spine descent stopped. Two arms, no `_` arm at any consumer.
pub enum Descent<'a, S: Scc, B> {
    /// `f` returned `Break(b)` at some node; descent stopped there.
    Broke(B),
    /// `f` returned `Continue` at this node and it has no SOLE child
    /// (`arity` 0, or `arity >= 2` — a spine descent cannot pick a
    /// branch).
    Exhausted(S::Node<'a>),
}

/// ALLOCATION-FREE spine descent: one loop variable, no [`ChunkStack`],
/// no `Vec`, no heap of any kind on any path. Applies `f` from `root`
/// down through [`sole_child`] while `f` continues.
pub fn descend_spine<'a, S: Scc, B>(
    root: S::Node<'a>,
    mut f: impl FnMut(S::Node<'a>) -> ControlFlow<B>,
) -> Descent<'a, S, B> {
    let w = Walk::new();
    let mut cur = root;
    loop {
        match f(cur) {
            ControlFlow::Break(b) => return Descent::Broke(b),
            ControlFlow::Continue(()) => match sole_child::<S>(cur) {
                Some(r) => cur = S::open(r, &w),
                None => return Descent::Exhausted(cur),
            },
        }
    }
}

/// Per-node control for [`find_preorder`]. Exhaustive at every driver, no
/// `_` arm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    /// Push this node's `child` edges.
    Descend,
    /// This node is a leaf for this walk: push nothing.
    Prune,
}

/// Pre-order DFS, left-to-right, with early break and pruning.
///
/// `Break(b)` stops the whole walk and returns `Some(b)`; [`Step::Prune`]
/// skips the node's children. Descends sole-child spines IN-LOOP and
/// touches the stack only at nodes with `arity >= 2`, so a spine-shaped
/// tree is walked with zero allocations.
pub fn find_preorder<'a, S: Scc, B>(
    root: S::Node<'a>,
    mut f: impl FnMut(S::Node<'a>) -> ControlFlow<B, Step>,
) -> Option<B> {
    let w = Walk::new();
    let mut stack: ChunkStack<S::Ref<'a>> = ChunkStack::new();
    let mut cur = Some(root);
    loop {
        let n = match cur.take() {
            Some(n) => n,
            None => match stack.pop() {
                Some(r) => S::open(r, &w),
                None => return None,
            },
        };
        match f(n) {
            ControlFlow::Break(b) => return Some(b),
            ControlFlow::Continue(Step::Prune) => {}
            ControlFlow::Continue(Step::Descend) => {
                if S::child(n, 1).is_some() {
                    push_children::<S>(n, &mut stack);
                } else if let Some(r) = S::child(n, 0) {
                    cur = Some(S::open(r, &w));
                }
            }
        }
    }
}

/// Opens one child of `n`, by index, left to right — O(1), no
/// allocation.
///
/// **Narrowing, stated because it is one.** Every other route to a child
/// reference is a driver loop; this one hands a consumer a single opened
/// child and therefore lets a consumer keep a hand-written recursion.
/// It exists for exactly one caller: `build_metric_node`
/// (`crates/pulsus-read/src/logql/plan.rs`), which issue #272 leaves
/// recursive by ruling and **#293** converts. Retyping SCC-2's slots
/// forces every `MetricExpr` consumer to change, and that function is a
/// consumer, so it cannot both stay recursive and stay untouched.
///
/// It adds no capability class C1 did not already concede: `#[derive]`,
/// `write!(f, "{child}")`, `child.clone()`, `child == other` and
/// `child.hash(state)` on a [`Child`] field remain compile errors, and
/// [`find_preorder`] already yields whole nodes to a callback that may
/// capture them. When #293 lands, its only caller goes away and so
/// should this function.
pub fn child_of<'a, S: Scc>(n: S::Node<'a>, i: usize) -> Option<S::Node<'a>> {
    let w = Walk::new();
    S::child(n, i).map(|r| S::open(r, &w))
}

/// Opens an N-ary child slot's contents as a slice, for the same single
/// consumer and with the same narrowing as [`child_of`].
pub fn slice_of<'a, T>(s: RefSlice<'a, T>) -> &'a [T] {
    let w = Walk::new();
    s.open(&w)
}

/// Visit-all pre-order, left-to-right. Same in-loop spine descent as
/// [`find_preorder`], from which it is derived.
pub fn preorder<'a, S: Scc>(root: S::Node<'a>, mut f: impl FnMut(S::Node<'a>)) {
    let found = find_preorder::<S, ()>(root, |n| {
        f(n);
        ControlFlow::Continue(Step::Descend)
    });
    match found {
        None => {}
        // Unreachable: the callback above never returns `Break`.
        Some(()) => unreachable!("preorder's callback never breaks"),
    }
}

/// Post-order, left-to-right, appended to `out`.
pub fn postorder_into<'a, S: Scc>(root: S::Node<'a>, out: &mut Vec<S::Node<'a>>) {
    let done: Result<(), ()> = try_postorder_into::<S, ()>(root, out, |_| Ok(()));
    match done {
        Ok(()) => {}
        // Unreachable: the charge above never fails.
        Err(()) => unreachable!("an uncharged post-order cannot fail"),
    }
}

/// Post-order, left-to-right, appended to `out`, **charging every
/// allocating step before it happens**.
///
/// `charge` is called with an upper bound on the bytes the next growth
/// will request from the global allocator — both the work stack's next
/// chunk (via [`ChunkStack::next_push_bytes`]) and `out`'s next
/// reallocation — and may refuse. Nothing is allocated after a refusal,
/// which is what makes a caller's pre-allocation ceiling actually cover
/// this walk rather than merely precede it.
pub fn try_postorder_into<'a, S: Scc, E>(
    root: S::Node<'a>,
    out: &mut Vec<S::Node<'a>>,
    mut charge: impl FnMut(usize) -> Result<(), E>,
) -> Result<(), E> {
    let w = Walk::new();
    let start = out.len();
    // A childless root is the overwhelmingly common shape (a leaf
    // filter, a scalar node). It needs no work stack at all, and
    // allocating one would put a chunk plus its index on every such
    // walk — which is how a driver silently moves an allocation band.
    if S::child(root, 0).is_none() {
        charge(next_vec_push_bytes(out))?;
        out.push(root);
        return Ok(());
    }
    let mut stack: ChunkStack<S::Ref<'a>> = ChunkStack::new();
    charge(stack.next_push_bytes())?;
    stack.push(S::wrap(root));
    while let Some(r) = stack.pop() {
        let n = S::open(r, &w);
        charge(next_vec_push_bytes(out))?;
        out.push(n);
        // LEFT-to-RIGHT here, so the reversal below yields post-order
        // left-to-right.
        let mut i = 0;
        while let Some(c) = S::child(n, i) {
            charge(stack.next_push_bytes())?;
            stack.push(c);
            i += 1;
        }
    }
    out[start..].reverse();
    Ok(())
}

/// A conservative upper bound on the bytes `v`'s next `push` will request
/// from the global allocator: zero while spare capacity remains, else the
/// capacity `Vec` will choose (`0 -> 4`, else `2 x cap`) plus one
/// allocation envelope. Computed before the call, in release as in debug.
#[inline]
fn next_vec_push_bytes<T>(v: &Vec<T>) -> usize {
    if v.len() < v.capacity() {
        return 0;
    }
    let next_cap = if v.capacity() == 0 {
        INDEX_INIT
    } else {
        2 * v.capacity()
    };
    next_cap * mem::size_of::<T>() + WALK_ALLOC_ENVELOPE_BYTES
}

/// Structural equality over a pair stack. `PartialEq` has no side
/// effects, so only the verdict is observable and the verdict is
/// order-independent.
pub fn eq_iter<'a, 'b, S: Scc>(a: S::Node<'a>, b: S::Node<'b>) -> bool {
    let w = Walk::new();
    if S::child(a, 0).is_none() && S::child(b, 0).is_none() {
        return S::shallow_eq(a, b);
    }
    let mut stack: ChunkStack<(S::Ref<'a>, S::Ref<'b>)> = ChunkStack::new();
    stack.push((S::wrap(a), S::wrap(b)));
    while let Some((ra, rb)) = stack.pop() {
        let na = S::open(ra, &w);
        let nb = S::open(rb, &w);
        if !S::shallow_eq(na, nb) {
            return false;
        }
        let mut i = 0;
        loop {
            match (S::child(na, i), S::child(nb, i)) {
                (Some(ca), Some(cb)) => stack.push((ca, cb)),
                (None, None) => break,
                // `shallow_eq` compares arity, so this is defensive
                // rather than reachable; it is a pure predicate, so the
                // correct answer is `false`, never a panic.
                (Some(_), None) | (None, Some(_)) => return false,
            }
            i += 1;
        }
    }
    true
}

/// Post-order clone: collect the nodes bottom-up, then
/// [`Scc::rebuild`] each one from the values its children already left on
/// the tail of the value vector.
pub fn clone_iter<S: Scc>(root: S::Node<'_>) -> S::Val {
    // Leaf fast path: no traversal, no value stack, no allocation beyond
    // whatever `rebuild` clones. `Vec::new()` does not allocate.
    if S::child(root, 0).is_none() {
        let mut childless: Vec<S::Val> = Vec::new();
        return S::rebuild(root, &mut childless);
    }
    let mut nodes: Vec<S::Node<'_>> = Vec::new();
    postorder_into::<S>(root, &mut nodes);
    let mut vals: Vec<S::Val> = Vec::with_capacity(nodes.len());
    for n in nodes {
        let v = S::rebuild(n, &mut vals);
        vals.push(v);
    }
    match vals.pop() {
        Some(v) if vals.is_empty() => v,
        // Unreachable: a post-order rebuild consumes exactly `arity(n)`
        // values per node and pushes one, so exactly one value survives.
        _ => unreachable!("post-order rebuild leaves exactly one root value"),
    }
}

/// Whether any child of `n` has a child of its own.
///
/// This is the emptied-node fast path's predicate: when it is false the
/// compiler's own glue costs at most one extra frame, so no work stack is
/// needed — which is what keeps a placeholder's drop free of the
/// one-allocation-per-node cost the flattening exists to avoid.
#[inline]
fn has_grandchild<S: Scc>(n: S::Node<'_>, w: &Walk) -> bool {
    let mut i = 0;
    while let Some(c) = S::child(n, i) {
        if S::child(S::open(c, w), 0).is_some() {
            return true;
        }
        i += 1;
    }
    false
}

/// Iterative drop.
///
/// Pops a value, releases its own fields, steals its children onto the
/// work stack (right-to-left, so pop order is left-to-right) and then
/// drops the emptied value — whose own `Drop::drop` re-enters here, sees
/// only placeholder children and returns on the fast path. The visit
/// sequence is pre-order DFS, exactly compiler glue order (a parent's
/// `Drop::drop` runs before its fields drop).
///
/// **Only nodes that have children are emptied.** A leaf value popped off
/// the work stack is dropped untouched, so its own fields are still
/// intact when its `Drop::drop` observes it — which is what keeps a real
/// leaf distinguishable from the same-kind placeholder.
///
/// Called **only** from an `impl Drop` body (census check (h)).
pub fn dismantle<S: Scc>(root: S::Slot<'_>) {
    let w = Walk::new();
    let mut node = S::open_slot(root, &w);

    let mut stack: ChunkStack<S::Val> = ChunkStack::new();
    {
        // The fast path is read off the node BEFORE anything is taken.
        let flat = {
            let r = S::slot_node_ref(&node);
            !has_grandchild::<S>(r, &w)
        };
        if flat {
            return;
        }
        S::take_own_fields(&mut node);
        S::steal_children(&mut node, &mut stack);
    }

    while let Some(mut v) = stack.pop() {
        let has_child = {
            let n = S::open(S::val_ref(&v), &w);
            S::child(n, 0).is_some()
        };
        if has_child {
            let mut sn = S::open_slot(S::val_slot(&mut v), &w);
            S::take_own_fields(&mut sn);
            S::steal_children(&mut sn, &mut stack);
        }
        // The emptied value's own `Drop::drop` runs here — recording its
        // visit and re-entering `dismantle`, which takes the fast path.
        drop(v);
    }
}

// ---------------------------------------------------------------------
// Emitters
// ---------------------------------------------------------------------

/// The per-step outcome of an emitter machine.
pub enum Emit<'a, S: Scc> {
    /// This frame is finished.
    Done,
    /// Re-push this frame at `next_step`, then run `child` from
    /// `child_step` at `child_level`.
    ///
    /// `child_level` is the child's **indentation level**, chosen by the
    /// parent rather than derived from the driver's frame depth: a list
    /// element sits one level deeper than the field that holds the list,
    /// so depth and level are not the same quantity.
    ///
    /// `child` may be the frame's own node (`S::wrap(n)`): that is how a
    /// wrapper step — `Display`'s operand parenthesisation, say — brackets
    /// a node's own rendering without a second machine. It terminates for
    /// the same reason every other step does, because the entered step
    /// never returns to the wrapper step.
    Descend {
        next_step: u32,
        child: S::Ref<'a>,
        child_step: u32,
        child_level: usize,
    },
}

/// Drives a type's `Debug`/`Display`/`Hash` step machine.
///
/// `step` receives an **opened node**, the frame's step index and the
/// frame's indentation level, and returns the next action — never a
/// token. The sink is whatever the caller captured (`Formatter`,
/// `Hasher`, …), so no associated-type gymnastics are needed to carry it.
///
/// The root runs at level 0; every other frame's level is the one its
/// parent chose in [`Emit::Descend`].
pub fn emit<'a, S: Scc, E>(
    root: S::Node<'a>,
    mut step: impl FnMut(S::Node<'a>, u32, usize) -> Result<Emit<'a, S>, E>,
) -> Result<(), E> {
    let w = Walk::new();
    let mut stack: ChunkStack<(S::Ref<'a>, u32, usize)> = ChunkStack::new();
    // The current frame is held OUTSIDE the stack, so a machine that
    // finishes without ever descending — every leaf `Debug`, `Display`
    // and `Hash` — touches no heap at all.
    let mut cur = Some((S::wrap(root), 0u32, 0usize));
    loop {
        let (r, st, level) = match cur.take() {
            Some(f) => f,
            None => match stack.pop() {
                Some(f) => f,
                None => return Ok(()),
            },
        };
        let n = S::open(r, &w);
        match step(n, st, level)? {
            Emit::Done => {}
            Emit::Descend {
                next_step,
                child,
                child_step,
                child_level,
            } => {
                stack.push((r, next_step, level));
                cur = Some((child, child_step, child_level));
            }
        }
    }
}

/// Inserts `4 * level` spaces after every `\n` written through it — the
/// only place alternate-mode padding is produced, used ONLY to render a
/// node's non-recursive own fields (`{v:#?}`) at a known depth.
pub struct PadWriter<'a, W: fmt::Write> {
    inner: &'a mut W,
    level: usize,
}

impl<'a, W: fmt::Write> PadWriter<'a, W> {
    #[inline]
    pub fn new(inner: &'a mut W, level: usize) -> Self {
        PadWriter { inner, level }
    }
}

impl<W: fmt::Write> fmt::Write for PadWriter<'_, W> {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        let mut first = true;
        for part in s.split('\n') {
            if !first {
                self.inner.write_str("\n")?;
                for _ in 0..self.level {
                    self.inner.write_str("    ")?;
                }
            }
            first = false;
            self.inner.write_str(part)?;
        }
        Ok(())
    }
}

impl<W: fmt::Write> fmt::Debug for PadWriter<'_, W> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PadWriter")
            .field("level", &self.level)
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------
// Derive-equivalent `Debug` chunk helpers
// ---------------------------------------------------------------------
//
// `#[derive(Debug)]`'s alternate layout indents nested values by
// `4 * level`, produced by `std`'s own `PadAdapter` composing one level
// per `debug_struct`/`debug_tuple`. A step machine writes its literal
// chunks itself, so it needs the same four primitives — shared here so
// every converted type writes byte-identical padding.

/// Writes `,` + the separator that precedes the next field of a struct or
/// tuple body.
pub fn dbg_sep(f: &mut fmt::Formatter<'_>, alt: bool, level: usize) -> fmt::Result {
    if alt {
        f.write_str(",\n")?;
        write_pad(f, level + 1)
    } else {
        f.write_str(", ")
    }
}

pub fn write_pad(f: &mut dyn fmt::Write, level: usize) -> fmt::Result {
    for _ in 0..level {
        f.write_str("    ")?;
    }
    Ok(())
}

/// Renders one non-recursive own field's value at `level`, matching the
/// derive's nested indentation exactly.
pub fn dbg_own<T: fmt::Debug>(
    f: &mut fmt::Formatter<'_>,
    v: &T,
    alt: bool,
    level: usize,
) -> fmt::Result {
    if alt {
        let mut pad = PadWriter::new(f, level);
        write!(pad, "{v:#?}")
    } else {
        write!(f, "{v:?}")
    }
}

/// Opens a struct body: `Name {` plus, in alternate mode, the newline and
/// padding that precede its first field.
pub fn dbg_open_struct(
    f: &mut fmt::Formatter<'_>,
    name: &str,
    alt: bool,
    level: usize,
) -> fmt::Result {
    if alt {
        f.write_str(name)?;
        f.write_str(" {\n")?;
        write_pad(f, level + 1)
    } else {
        write!(f, "{name} {{ ")
    }
}

pub fn dbg_close_struct(f: &mut fmt::Formatter<'_>, alt: bool, level: usize) -> fmt::Result {
    if alt {
        f.write_str(",\n")?;
        write_pad(f, level)?;
        f.write_str("}")
    } else {
        f.write_str(" }")
    }
}

pub fn dbg_open_tuple(
    f: &mut fmt::Formatter<'_>,
    name: &str,
    alt: bool,
    level: usize,
) -> fmt::Result {
    if alt {
        f.write_str(name)?;
        f.write_str("(\n")?;
        write_pad(f, level + 1)
    } else {
        write!(f, "{name}(")
    }
}

pub fn dbg_close_tuple(f: &mut fmt::Formatter<'_>, alt: bool, level: usize) -> fmt::Result {
    if alt {
        f.write_str(",\n")?;
        write_pad(f, level)?;
        f.write_str(")")
    } else {
        f.write_str(")")
    }
}

// ---------------------------------------------------------------------
// Zero-cost assertions (C1 support; AC 7)
// ---------------------------------------------------------------------

const _: () = assert!(mem::size_of::<Walk>() == 0);
const _: () = assert!(mem::size_of::<Child<u64>>() == mem::size_of::<Box<u64>>());
const _: () = assert!(mem::size_of::<ChildVec<u64>>() == mem::size_of::<Vec<u64>>());
const _: () = assert!(mem::size_of::<Ref<'_, u64>>() == mem::size_of::<&u64>());
const _: () = assert!(mem::size_of::<Slot<'_, u64>>() == mem::size_of::<&mut u64>());
