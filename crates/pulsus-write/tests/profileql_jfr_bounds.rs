//! Issue #819, code review round 1: a corrupt JFR body is answered, never
//! a hang or an unbounded allocation. Each case is a crafted recording
//! that a reader driven by the body's own offsets, counts or ids would
//! loop on, grow on, or degrade quadratically on.
//!
//! Every parse runs on its own thread with a deadline, so a hang fails as
//! an assertion; this binary's allocator refuses to hold more than
//! [`CAP`] bytes at once, so unbounded growth aborts the test.

use std::alloc::{GlobalAlloc, Layout, System};
use std::io::Read as _;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::time::Duration;

use pulsus_write::protocols::profileql_legacy::jfr::{JfrProfiles, LabelsSnapshot, parse_jfr};

/// The most this test binary holds at once.
const CAP: usize = 1 << 30;

struct CappedAlloc;

static LIVE: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for CappedAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if LIVE.fetch_add(layout.size(), Ordering::Relaxed) + layout.size() > CAP {
            LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
            return std::ptr::null_mut();
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: CappedAlloc = CappedAlloc;

/// How long one parse may take, in a debug build.
const DEADLINE: Duration = Duration::from_secs(10);

/// Parses `body` on its own thread; fails when it panics or does not
/// answer within [`DEADLINE`].
fn parse_within(what: &str, body: Vec<u8>) -> Result<JfrProfiles, String> {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let out = parse_jfr(&body, &LabelsSnapshot::default(), 0, 0, 100);
        let _ = tx.send(out);
    });
    match rx.recv_timeout(DEADLINE) {
        Ok(out) => out,
        Err(mpsc::RecvTimeoutError::Timeout) => {
            panic!("{what}: the parse did not answer within {DEADLINE:?}")
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => panic!("{what}: the parse panicked"),
    }
}

// ---------------------------------------------------------------------
// A JFR writer, as far as the reader reads
// ---------------------------------------------------------------------

const INT: i64 = 10;
const LONG: i64 = 11;
const SHORT: i64 = 12;
const FLOAT: i64 = 13;
const BOOLEAN: i64 = 14;
const STRING: i64 = 15;
const FRAME_TYPE: i64 = 20;
const THREAD_STATE: i64 = 21;
const THREAD: i64 = 22;
const CLASS: i64 = 23;
const METHOD: i64 = 24;
const PACKAGE: i64 = 25;
const SYMBOL: i64 = 26;
const STACK_TRACE: i64 = 27;
const CLASS_LOADER: i64 = 28;
const STACK_FRAME: i64 = 29;
const EXECUTION_SAMPLE: i64 = 100;
const ACTIVE_SETTING: i64 = 101;

/// A field: name, type id, constant pool, array.
type Field = (&'static str, i64, bool, bool);
type Class = (i64, &'static str, Vec<Field>);

fn varint(out: &mut Vec<u8>, mut v: u64) {
    loop {
        let b = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(b);
            return;
        }
        out.push(b | 0x80);
    }
}

/// `v` as a five-byte varint, so a size can be written before it is known.
fn padded5(v: u64) -> [u8; 5] {
    let mut b = [0u8; 5];
    for (i, slot) in b.iter_mut().enumerate() {
        *slot = ((v >> (7 * i)) & 0x7f) as u8 | if i < 4 { 0x80 } else { 0 };
    }
    b
}

/// `v` as a nine-byte varlong, the ninth byte whole: any 64-bit value.
fn varlong9(v: u64) -> [u8; 9] {
    let mut b = [0u8; 9];
    for (i, slot) in b.iter_mut().enumerate().take(8) {
        *slot = ((v >> (7 * i)) & 0x7f) as u8 | 0x80;
    }
    b[8] = (v >> 56) as u8;
    b
}

fn utf8(out: &mut Vec<u8>, s: &str) {
    out.push(3);
    varint(out, s.len() as u64);
    out.extend_from_slice(s.as_bytes());
}

/// A record whose first field is its own size, five bytes wide.
fn sized(content: Vec<u8>) -> Vec<u8> {
    let mut out = padded5(content.len() as u64 + 5).to_vec();
    out.extend(content);
    out
}

/// Every type the reader requires, the stack frame's fields as given.
fn classes(frame_fields: Vec<Field>) -> Vec<Class> {
    vec![
        (INT, "int", vec![]),
        (LONG, "long", vec![]),
        (SHORT, "short", vec![]),
        (FLOAT, "float", vec![]),
        (BOOLEAN, "boolean", vec![]),
        (STRING, "java.lang.String", vec![]),
        (
            FRAME_TYPE,
            "jdk.types.FrameType",
            vec![("description", STRING, false, false)],
        ),
        (
            THREAD_STATE,
            "jdk.types.ThreadState",
            vec![("name", STRING, false, false)],
        ),
        (
            THREAD,
            "java.lang.Thread",
            vec![("osName", STRING, false, false)],
        ),
        (
            CLASS,
            "java.lang.Class",
            vec![("name", SYMBOL, true, false)],
        ),
        (
            METHOD,
            "jdk.types.Method",
            vec![("type", CLASS, true, false), ("name", SYMBOL, true, false)],
        ),
        (
            PACKAGE,
            "jdk.types.Package",
            vec![("name", SYMBOL, true, false)],
        ),
        (
            SYMBOL,
            "jdk.types.Symbol",
            vec![("string", STRING, false, false)],
        ),
        (
            STACK_TRACE,
            "jdk.types.StackTrace",
            vec![
                ("truncated", BOOLEAN, false, false),
                ("frames", STACK_FRAME, false, true),
            ],
        ),
        (
            CLASS_LOADER,
            "jdk.types.ClassLoader",
            vec![("name", SYMBOL, true, false)],
        ),
        (STACK_FRAME, "jdk.types.StackFrame", frame_fields),
        (
            EXECUTION_SAMPLE,
            "jdk.ExecutionSample",
            vec![
                ("stackTrace", STACK_TRACE, true, false),
                ("state", THREAD_STATE, true, false),
                ("contextId", LONG, false, false),
            ],
        ),
        (
            ACTIVE_SETTING,
            "jdk.ActiveSetting",
            vec![
                ("name", STRING, false, false),
                ("value", STRING, false, false),
            ],
        ),
    ]
}

fn frame_fields() -> Vec<Field> {
    vec![
        ("method", METHOD, true, false),
        ("lineNumber", INT, false, false),
    ]
}

/// The metadata event: `extra` more strings in its table beside the names.
fn metadata(classes: &[Class], extra: usize) -> Vec<u8> {
    let mut strings: Vec<String> = Vec::new();
    let mut at = |s: &str| -> u64 {
        if let Some(i) = strings.iter().position(|t| t == s) {
            return i as u64;
        }
        strings.push(s.to_string());
        (strings.len() - 1) as u64
    };
    let mut elements = Vec::new();
    let element = |out: &mut Vec<u8>, name: u64, attrs: &[(u64, u64)], children: u64| {
        varint(out, name);
        varint(out, attrs.len() as u64);
        for (k, v) in attrs {
            varint(out, *k);
            varint(out, *v);
        }
        varint(out, children);
    };
    let (root, meta, region) = (at("root"), at("metadata"), at("region"));
    element(&mut elements, root, &[], 2);
    element(&mut elements, meta, &[], classes.len() as u64);
    for (id, name, fields) in classes {
        let attrs = [(at("id"), at(&id.to_string())), (at("name"), at(name))];
        element(&mut elements, at("class"), &attrs, fields.len() as u64);
        for (fname, ty, cp, array) in fields {
            let mut attrs = vec![
                (at("name"), at(fname)),
                (at("class"), at(&ty.to_string())),
                (at("constantPool"), at(if *cp { "true" } else { "false" })),
            ];
            if *array {
                attrs.push((at("dimension"), at("1")));
            }
            element(&mut elements, at("field"), &attrs, 0);
        }
    }
    element(&mut elements, region, &[], 0);
    for i in 0..extra {
        strings.push(format!("s{i}"));
    }
    let mut content = Vec::new();
    varint(&mut content, 0);
    content.extend_from_slice(&[0, 0, 0]);
    varint(&mut content, strings.len() as u64);
    for s in &strings {
        utf8(&mut content, s);
    }
    content.extend(elements);
    sized(content)
}

/// A constant-pool event: its delta, then each pool's class, count and
/// entries.
fn pool_event(delta: i64, pools: &[(i64, u64, Vec<u8>)]) -> Vec<u8> {
    let mut content = Vec::new();
    varint(&mut content, 1);
    content.extend_from_slice(&[0, 0]);
    content.extend_from_slice(&varlong9(delta as u64));
    content.push(0);
    varint(&mut content, pools.len() as u64);
    for (class, n, entries) in pools {
        varint(&mut content, *class as u64);
        varint(&mut content, *n);
        content.extend_from_slice(entries);
    }
    sized(content)
}

/// A chunk header.
fn header(size: i64, cp: i64, meta: i64) -> Vec<u8> {
    let mut h = Vec::with_capacity(68);
    h.extend_from_slice(&0x464c_5200u32.to_be_bytes());
    h.extend_from_slice(&0x0002_0000u32.to_be_bytes());
    for v in [size, cp, meta, 0, 0, 0, 1_000_000_000] {
        h.extend_from_slice(&v.to_be_bytes());
    }
    h.extend_from_slice(&0u32.to_be_bytes());
    h
}

/// One chunk: the metadata, the pool events in order, then `events`.
fn recording(meta: Vec<u8>, pools: Vec<Vec<u8>>, events: &[u8]) -> Vec<u8> {
    let cp = 68 + meta.len();
    let size = cp + pools.iter().map(Vec::len).sum::<usize>() + events.len();
    let mut out = header(size as i64, cp as i64, 68);
    out.extend(meta);
    for p in pools {
        out.extend(p);
    }
    out.extend_from_slice(events);
    out
}

/// The pools of one valid stack, id 1: `A.m`, `frames` frames deep, and
/// thread state 1, `STATE_RUNNABLE`.
fn one_deep_stack(frames: u64) -> Vec<(i64, u64, Vec<u8>)> {
    let mut symbols = Vec::new();
    for (id, s) in [(1, "A"), (2, "m")] {
        varint(&mut symbols, id);
        utf8(&mut symbols, s);
    }
    let mut states = Vec::new();
    varint(&mut states, 1);
    utf8(&mut states, "STATE_RUNNABLE");
    let mut stack = vec![1, 0];
    varint(&mut stack, frames);
    for _ in 0..frames {
        stack.extend_from_slice(&[1, 0]);
    }
    vec![
        (SYMBOL, 2, symbols),
        (CLASS, 1, vec![1, 1]),
        (METHOD, 1, vec![1, 1, 2]),
        (THREAD_STATE, 1, states),
        (STACK_TRACE, 1, stack),
    ]
}

// ---------------------------------------------------------------------
// The cases
// ---------------------------------------------------------------------

/// The writer's own recording reads, so a case's failure is its defect.
#[test]
fn a_written_recording_reads() {
    let mut events = Vec::new();
    for ctx in 0..3u64 {
        let mut e = vec![EXECUTION_SAMPLE as u8, 1, 1];
        varint(&mut e, ctx);
        events.push(e.len() as u8 + 1);
        events.extend(e);
    }
    let body = recording(
        metadata(&classes(frame_fields()), 0),
        vec![pool_event(0, &one_deep_stack(2))],
        &events,
    );
    let out = parse_within("valid", body).expect("a valid recording");
    assert_eq!(out.profiles.len(), 1);
    assert_eq!(out.profiles[0].1.sample.len(), 3);
}

/// Code review r1 finding 1: two constant-pool events whose deltas point at
/// each other.
#[test]
fn b1_constant_pool_events_that_point_at_each_other_are_refused() {
    let first = pool_event(0, &[]);
    let d = first.len() as i64;
    let body = recording(
        metadata(&classes(frame_fields()), 0),
        vec![pool_event(d, &[]), pool_event(-d, &[])],
        &[],
    );
    let out = parse_within("a pool cycle", body);
    assert!(out.is_err(), "{:?}", out.map(|o| o.profiles.len()));
}

/// Code review r1 finding 2: a stack frame type with no fields reads no
/// byte per frame, so a declared count of 2^32 − 1 frames costs no input.
#[test]
fn b2_frames_that_read_no_byte_are_refused() {
    let mut stack = vec![1, 0];
    varint(&mut stack, u64::from(u32::MAX));
    let body = recording(
        metadata(&classes(Vec::new()), 0),
        vec![pool_event(0, &[(STACK_TRACE, 1, stack)])],
        &[],
    );
    let out = parse_within("frames of no field", body);
    assert!(out.is_err(), "{:?}", out.map(|o| o.profiles.len()));
}

/// Chunks of 69 bytes, a header and a one-byte event, whose metadata
/// offsets reach past their own end to one large metadata event, which
/// every chunk would read again.
#[test]
fn b3_a_chunks_metadata_and_pools_lie_inside_the_chunk() {
    let chunks = 3_000usize;
    let meta = metadata(&classes(frame_fields()), 200_000);
    let at_meta = (chunks * 69) as i64;
    let at_cp = at_meta + meta.len() as i64;
    let mut body = Vec::new();
    for i in 0..chunks {
        let here = (i * 69) as i64;
        body.extend(header(69, at_cp - here, at_meta - here));
        body.push(1);
    }
    body.extend(meta);
    body.extend(pool_event(0, &[]));
    let out = parse_within("chunks reading one far metadata", body);
    assert!(out.is_err(), "{:?}", out.map(|o| o.profiles.len()));
}

/// Six-byte events whose one string field declares two million
/// characters, so each would read on through every event after it.
#[test]
fn b4_an_events_fields_lie_inside_the_event() {
    let mut events = Vec::new();
    for _ in 0..600_000 {
        events.extend_from_slice(&[6, ACTIVE_SETTING as u8, 4, 0xff, 0xff, 0x7f]);
    }
    let mut types = classes(frame_fields());
    for c in &mut types {
        if c.0 == ACTIVE_SETTING {
            c.2 = vec![("name", STRING, false, false)];
        }
    }
    let body = recording(
        metadata(&types, 0),
        vec![pool_event(0, &one_deep_stack(1))],
        &events,
    );
    let out = parse_within("events reading past themselves", body);
    assert!(out.is_err(), "{:?}", out.map(|o| o.profiles.len()));
}

/// A stack 100,000 frames deep and 200,000 samples of it, each its own
/// sample by its context id: the profile is far over the size limit long
/// before its samples are all built.
#[test]
fn b5_samples_stop_growing_once_the_profile_is_over_the_size_limit() {
    let mut events = Vec::new();
    for ctx in 0..200_000u64 {
        let mut e = vec![EXECUTION_SAMPLE as u8, 1, 1];
        varint(&mut e, ctx);
        events.push(e.len() as u8 + 1);
        events.extend(e);
    }
    let body = recording(
        metadata(&classes(frame_fields()), 0),
        vec![pool_event(0, &one_deep_stack(100_000))],
        &events,
    );
    let out = parse_within("a deep stack sampled often", body).expect("a valid recording");
    let p = &out.profiles[0].1;
    let ids: usize = p.sample.iter().map(|s| s.location_id.len()).sum();
    assert!(ids <= 8 << 20, "{ids} location ids held");
}

/// 400,000 stack traces whose ids share their low 32 bits: a hash of the
/// id alone puts every one in the same bucket.
#[test]
fn b6_pool_ids_chosen_to_collide_do_not_slow_the_read() {
    let n = 400_000u64;
    let mut stacks = Vec::new();
    for i in 1..=n {
        varint(&mut stacks, i << 32);
        stacks.extend_from_slice(&[0, 0]);
    }
    let body = recording(
        metadata(&classes(frame_fields()), 0),
        vec![pool_event(0, &[(STACK_TRACE, n, stacks)])],
        &[],
    );
    let out = parse_within("colliding pool ids", body);
    assert!(out.is_ok(), "{:?}", out.map(|o| o.profiles.len()));
}

fn vendored(name: &str) -> Vec<u8> {
    let path = format!(
        "{}/tests/fixtures/profileql_jfr/{name}",
        env!("CARGO_MANIFEST_DIR")
    );
    let gz = std::fs::read(&path).unwrap_or_else(|e| panic!("{path}: {e}"));
    let mut out = Vec::new();
    flate2::read::MultiGzDecoder::new(&gz[..])
        .read_to_end(&mut out)
        .expect("a gzip file");
    out
}

/// Truncated and bit-flipped copies of the vendored recordings: each is
/// answered, `Ok` or `Err`, within the deadline and without a panic.
#[test]
fn f1_truncated_and_bit_flipped_recordings_are_answered() {
    let mut seed: u64 = 0x819;
    let mut next = move || {
        seed = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        seed >> 33
    };
    for name in ["example.jfr.gz", "dump1.jfr.gz", "wall.jfr.gz"] {
        let body = vendored(name);
        for i in 1..=24 {
            let cut = body.len() * i / 25;
            let _ = parse_within(&format!("{name} cut at {cut}"), body[..cut].to_vec());
        }
        for _ in 0..120 {
            let mut flipped = body.clone();
            let at = next() as usize % flipped.len();
            flipped[at] ^= 1 << (next() % 8);
            let _ = parse_within(&format!("{name} flipped at {at}"), flipped);
        }
    }
}
