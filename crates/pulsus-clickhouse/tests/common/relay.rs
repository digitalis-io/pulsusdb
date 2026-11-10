//! A TCP forwarder to one server, on an ephemeral loopback port (issue
//! #803). Switched off, it shuts every open connection and every new one,
//! so a client reading through it meets a server failure mid-request.

#![allow(dead_code)]

use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

struct Shared {
    on: AtomicBool,
    stop: AtomicBool,
    open: Mutex<Vec<TcpStream>>,
}

pub struct Relay {
    pub addr: SocketAddr,
    shared: Arc<Shared>,
    handle: Option<thread::JoinHandle<()>>,
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    match m.lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    }
}

impl Relay {
    /// Forwards to `host:http_port`.
    pub fn start(host: &str, http_port: u16) -> Self {
        let target = (host, http_port)
            .to_socket_addrs()
            .expect("resolve the relay target")
            .next()
            .expect("the relay target has an address");
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind the relay");
        let addr = listener.local_addr().expect("local_addr");
        listener.set_nonblocking(true).expect("nonblocking");
        let shared = Arc::new(Shared {
            on: AtomicBool::new(true),
            stop: AtomicBool::new(false),
            open: Mutex::new(Vec::new()),
        });
        let accept_shared = Arc::clone(&shared);
        let handle = thread::spawn(move || {
            while !accept_shared.stop.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((client, _)) => {
                        client.set_nonblocking(false).ok();
                        if !accept_shared.on.load(Ordering::Relaxed) {
                            let _ = client.shutdown(Shutdown::Both);
                            continue;
                        }
                        let Ok(upstream) = TcpStream::connect(target) else {
                            let _ = client.shutdown(Shutdown::Both);
                            continue;
                        };
                        forward(client, upstream, &accept_shared);
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                    }
                    Err(_) => break,
                }
            }
        });
        Self {
            addr,
            shared,
            handle: Some(handle),
        }
    }

    /// From now on every open and every new connection is shut.
    pub fn switch_off(&self) {
        self.shared.on.store(false, Ordering::Relaxed);
        for s in lock(&self.shared.open).drain(..) {
            let _ = s.shutdown(Shutdown::Both);
        }
    }
}

impl Drop for Relay {
    fn drop(&mut self) {
        self.shared.stop.store(true, Ordering::Relaxed);
        self.switch_off();
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

/// Copies both ways on two threads, keeping a handle on each side so
/// [`Relay::switch_off`] can shut them.
fn forward(client: TcpStream, upstream: TcpStream, shared: &Arc<Shared>) {
    {
        let mut open = lock(&shared.open);
        if let Ok(c) = client.try_clone() {
            open.push(c);
        }
        if let Ok(u) = upstream.try_clone() {
            open.push(u);
        }
    }
    let (Ok(c2), Ok(u2)) = (client.try_clone(), upstream.try_clone()) else {
        return;
    };
    thread::spawn(move || copy(client, u2));
    thread::spawn(move || copy(upstream, c2));
}

fn copy(mut from: TcpStream, mut to: TcpStream) {
    let mut buf = [0u8; 16 * 1024];
    loop {
        match from.read(&mut buf) {
            Ok(0) | Err(_) => break,
            Ok(n) => {
                if to.write_all(&buf[..n]).is_err() {
                    break;
                }
            }
        }
    }
    let _ = to.shutdown(Shutdown::Both);
    let _ = from.shutdown(Shutdown::Both);
}
