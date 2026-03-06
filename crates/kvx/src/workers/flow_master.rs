// Listens to a channel
// Receives messages from the channel
// uses a regulator to determine what the new output should be
// Updates that output, using an Arc<>
// There can only be one type of regulator in action
// when it's a CPU regulator, flow_master does the polling on the cpu_gauge, in a loop
// when it's a request latency regulator, flow_master does the polling on an mpsc channel.
// this channel is provided to drainers, and they publish to this channel information about what is occuring.
// 