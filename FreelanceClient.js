const ZMQ = require('zeromq');
const Queue = require('@limeeng/fifo-queue')


class Request {
    constructor(msg_id, msg, now) {
        this.msg_id = msg_id;
        this.msg = msg;
        this.left_retries = REQUEST_RETRIES;
        this.expires = new Date(now + 3_000);
    }
}

class Server {
    constructor(address) {
        this.address = address;
        this.alive = false;
        this.connected = false;
        this.is_last_operation_receive = false;
        const now = Date.now();
        this.ping_at = new Date(now + HEARTBEAT_INTERVAL);
        this.expires = new Date(now + HEARTBEAT_LIVENESS);
    }
}

const OUTBOUND_QUEUE_SIZE = 600_000;    // Queue to call external servers
const BATCH_NB = 15_000;

const REQUEST_RETRIES = 5;

const HEARTBEAT = "";
const HEARTBEAT_INTERVAL = 500; // milliseconds
const HEARTBEAT_LIVENESS = HEARTBEAT_INTERVAL * 3; //  If no server replies within this time, abandon request


class FreelanceAgent {

    request_queue = new Queue();
    reply_queue = new Queue();
    servers = {};            //  Servers we've connected to
    actives = [];            //  Servers we know are alive
    requests = new Map();
    data = Buffer.alloc(4 + 3 + 6);

    constructor() {
        this.reply_nb = 1;
        this.received_nb = 0;
        this.address = '';

        this.backend_socket = new ZMQ.Router();
        this.backend_socket.mandatory = true;
        this.backend_socket.receiveHighWaterMark = OUTBOUND_QUEUE_SIZE;
        this.backend_socket.sendHighWaterMark = OUTBOUND_QUEUE_SIZE;
    }

    on_command_message(endpoint) {
        console.log("I = connecting to", endpoint);
        this.backend_socket.connect(endpoint);
        this.servers[endpoint] = new Server(endpoint);
    }

    async on_request_message(now) {
        const item = this.request_queue.poll();
        if (item === undefined) {
            return -1;
        }
        const request_id = item[0];
        const request = new Request(request_id, item[1], now);
        this.requests[request_id] = request;
        await this.send_request(request);
        return request_id;
    }

    async send_request(request) {
        this.data.writeUIntLE(request.msg_id, 0, 4);
        this.data.write(request.msg, 4);
        const msg = this.data.slice(0, 4 + request.msg.length);
        await this.backend_socket.send([this.address, msg]);
    }

    async on_reply_message(now) {
        // ex: reply = [b'tcp://192.168.0.22:5555', b'157REQ124'] or [b'tcp://192.168.0.22:5555', b'']
        const [server_hostname, data] = await this.backend_socket.receive();
        const server = this.servers[String(server_hostname)];
        server.is_last_operation_receive = true;
        server.ping_at = new Date(now + HEARTBEAT_INTERVAL);
        server.expires = new Date(now + HEARTBEAT_LIVENESS);

        if (data.length === 0) {
            server.connected = true;
        } else {
            this.received_nb++;
            const request_id = data.readIntLE(0,4);
            const request = this.requests[request_id];
            if (request != null) {
                const reply = String(data.slice(4, data.length));
                this.send_reply(now, request, reply, server_hostname);
            }
        }

        if (!server.alive) {
            server.alive = true;
            console.log("I = SERVER ALIVE", server.address);
        }

        if (this.address === '') {
            this.address = server.address;
            this.actives.push(server);
        }
    }

    send_reply(now, request, reply, server_name) {
        delete this.requests[request.msg_id];
        this.reply_nb++;
        this.reply_queue.offer([request.msg_id, reply]);
    }

    async send_requests() {
        let j = 0;
        while (1) {
            const now = Date.now()

            if (this.actives.length > 0) {
                for (j=0; j<BATCH_NB; j++) {
                    if (await this.on_request_message(now) < 0) {
                        break;
                    }
                }
            }

            if (j === 0) {
                await sleep(1);
            }
        }
    }

    async read_replies() {
        while (1) {
            await this.on_reply_message(Date.now());
        }
    }
}

const sleep = (milliseconds) => {
    return new Promise(resolve => setTimeout(resolve, milliseconds))
}

class FreelanceClient {

    constructor() {
        this.agent = new FreelanceAgent();
    }

    async startSendRequests() {
        await this.agent.send_requests();
    }

    async startReadReplies() {
        await this.agent.read_replies();
    }

    stopAgent() {
        this.agent.isAlive = false;
    }

    async connect(endpoint) {
        this.agent.on_command_message(endpoint);
        await sleep(1);
    }

    sendRequest(request_id, request) {
        return this.agent.request_queue.offer([request_id, request]);
    }

    receiveReply() {
        return this.agent.reply_queue.poll();
    }
}
module.exports = {
    FreelanceClient
}
