
pub struct CpuPressure {}
pub struct DrainRate {}
pub struct ByteValue {}

pub enum Regulators {
    Static(ByteValue),
    // Given a CPU percent, regulate the number of bytes the manifold and joiner send to the drainer
    CpuPressureToDrainFlow(CpuPressure, ByteValue),
    // Given the latency of a request, regulate the number of bytes the manifold and joiner send
    DrainRateToManifoldFlow(DrainRate, ByteValue)
}



pub trait Regulate {
    // It needs to regulate itself hehehe
    fn regulate(&self, reading: usize, since_last_checked_ms: usize) -> usize;
}

impl Regulate for Regulators {
    fn regulate(&self, reading: usize, since_last_checked_ms: usize) -> usize {
        match self {

        }
    }
}
