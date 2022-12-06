const hive = require('hive-driver');
const { TCLIService, TCLIService_types } = hive.thrift;



const connection = new hive.connections.TcpConnection();
const auth = new hive.auth.PlainTcpAuthentication({
    username: 'hive',
    password: 'hive'
});

const client = new hive.HiveClient(
    TCLIService,
    TCLIService_types
);

module.exports = () => client.connect(
    {
        host: 'hive-server',
        port: 10000
    }, connection, auth);