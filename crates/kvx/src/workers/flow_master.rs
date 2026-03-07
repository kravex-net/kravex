use std::time::SystemTime;

// Listens to a channel
// Receives messages from the channel
// uses a regulator to determine what the new output should be
// Updates that output, using an Arc<>
// There can only be one type of regulator in action
// when it's a CPU regulator, flow_master does the polling on the cpu_gauge, in a loop
// when it's a request latency regulator, flow_master does the polling on an mpsc channel.
// this channel is provided to drainers, and they publish to this channel information about what is occuring.
use anyhow::{Context, Result};
use async_channel::{Receiver};
use tokio::task::JoinHandle;
use crate::GaugeReading;
use crate::regulators::{Regulate, Regulators};
use super::Worker;

pub struct FlowMaster {
    rx: Receiver<GaugeReading>,
    regulator: Regulators
}

impl FlowMaster {
    pub fn new(
        rx: Receiver<GaugeReading>,
        regulator: Regulators
    ) -> Self {
        Self {
            rx,
            regulator
        }
    }
}

impl Worker for FlowMaster {
    fn start(mut self) -> JoinHandle<Result<()>> {
        tokio::spawn(async move {
            let mut the_last_time_we_checked = SystemTime::now();
            loop {
                match self.rx.recv().await {
                    Ok(guage_reading) => {
                        let since_the_last_time_we_checked = SystemTime::now().duration_since(the_last_time_we_checked)?;
                        let the_reading = self.regulator.regulate(guage_reading, since_the_last_time_we_checked);
                        the_last_time_we_checked = SystemTime::now();
                    }
                    Err(_) => {
                        return Ok(());
                    }
                }
            }
        })
    }
}
