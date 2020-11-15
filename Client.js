const FreelanceClient = require("./FreelanceClient");

const REQUEST_NUMBER = 600_000;

function send_all_requests(client) {
    for (let request_nb = 1; request_nb < REQUEST_NUMBER+1; request_nb++) {
        client.sendRequest(request_nb, "REQ" + request_nb);
    }
    console.log("SEND REQUESTS FINISHED")
}

const sleep = (milliseconds) => {
    return new Promise(resolve => setTimeout(resolve, milliseconds))
}

async function read_all_replies(client) {

    let reply_nb = 0;
    let reply;

    while (true) {
        reply = client.receiveReply();
        if (reply === undefined) {
            await sleep(1);
        } else {
            reply_nb++;
            //console.log(reply[0], reply[1], reply_nb);
            if (reply_nb === REQUEST_NUMBER) {
                console.log("**************************** READ REPLIES FINISHED ****************************");
                break;
            }
        }
    }
}

async function main() {
    let client = new FreelanceClient.FreelanceClient();
    await client.connect("tcp://192.168.0.22:5555");
    send_all_requests(client);

    await Promise.all([
        client.startReadReplies(),
        client.startSendRequests(),
        read_all_replies(client),
    ]).then((values) => {
        console.log("stoping");
        client.stopAgent();
    });
}

main().catch(err => {
    console.error(err)
    process.exit(1)
})