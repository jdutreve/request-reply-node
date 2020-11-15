let zmq = require('zeromq');

async function main() {
    let server_socket = new zmq.Router();
    server_socket.routingId = 'tcp://192.168.0.22:5555';
    server_socket.receiveHighWaterMark = 1_000_000;
    server_socket.sendHighWaterMark = 1_000_000;
    server_socket.probeRouter = true;
    server_socket.mandatory = true;
    await server_socket.bind("tcp://*:5555");
    console.log("I: server is ready at tcp://192.168.0.22:5555");

    let start = 0;
    let counter = 0;
    let not_yet_started = true;

    for await (const [hostname, request] of server_socket) {
        counter++;
        if (not_yet_started) {
            not_yet_started = false;
            start = Date.now();
        } else if (counter % 100_000 === 0) {
            let duration = Date.now() - start;
            let rate = counter / duration;
            console.log("Count1: ", counter, ", Rate: ", rate);
        }
        await server_socket.send([hostname, request]);
    }
}

main().catch(err => {
    console.error(err)
    process.on('SIGINT', function() {
        server_socket.close();
    });
})
