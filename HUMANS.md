# Getting started

`cargo build` to build the solution, if it doesn't work, then let me know asap. I still need to setup CICD

# Style
I hope to put some humanity in the docs, comments, and code here. This project is for fun. So why not smile while doing so?

# Hopes and Dreams
Use as minimal dependencies
Hand crafted, like a delicous beer
AI for the bitch work

# Rant
agentic coding will eventually take over for commercial software development
full stop. it's fast. it can fuck.
I consider myself an artisan, craftsman with code.
Bits and bytes, syntax; to me is like saw dust to a carpenter. We master them.
AI is my powertools.
IDE is my handtools.
Lovingly caring over the feel of every line, this is my refuge from corporate america.
Back in my day... to pass a class, I had to write a doubly linked list, and a stack implementation, in C++, with no GC, no notes, no google, no AI, no docs. Practically hand written from scratch.
I'm trying to keep that vibe alive.

# Notes:
Global buffer pool which contains, and is inspired by c# array pool implementation (which is actually really really fast)
One thing to note is that we are probably coming across false sharing at the moment.
False sharing is a performance-degrading, unintentional cache synchronization issue occurring when multiple threads on different cores modify independent variables residing on the same 64-byte cache line. The CPU's cache coherence protocol (e.g., MESI) constantly invalidates and transfers this line between cores, creating a "ping-pong" effect that destroys parallel scalability
So we need to ensure that all of our data is 64 byte aligned. Maybe the enums need to be, too?

Source worker works on the source trait
  Within the source trait implementation, it will return a String of the raw result from the source
  This can be a buffer from reqwest, or a buffer we create (when reading from file system)
  We manage the buffers for this, and have a buffer pool.
  When using reqwest, the response body will use pub fn copy_to<W>(&mut self, w: &mut W) -> Result<u64>
  where
      W: Write + ?Sized,
Transform worker works on the transform trait
  When it's completed it's work on the transform, it returns the full page buffer back to the buffer pool to be re-used
  Within the transform trait, we do a single pass on the bytes of the page, and copy into a new buffer (also retrieved from the buffer pool). This new buffer is then sent into another mpmc channel to be handled.
  Transform workers should equal the number of CPU cores minus 1. This is so we can have one CPU core running tokio polling, and the others fully consumed with handling iteration over bytes of data, and copying from one buffer to another buffer.
Sink worker works on the sink trait
  The sink trait accepts the buffer from the buffer pool, and forwards it to the sink. In the case of reqwest, it sends bytes: ex:
  /// let bytes: Vec<u8> = vec![1, 10, 100];
      /// let client = reqwest::blocking::Client::new();
      /// let res = client.post("http://httpbin.org/post")
      ///     .body(bytes)
      ///     .send()?;
  