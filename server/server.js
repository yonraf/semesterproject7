'use strict';
const hive = require('hive-driver');
const {TCLIService_types } = hive.thrift;


const express = require('express');

const connection = require('./connection')


const utils = new hive.HiveUtils(
    TCLIService_types
);


// Constants
const PORT = 3000;
const HOST = '0.0.0.0';

// App
const app = express();
app.get('/', (req, res) => {
    connection().then( async client => {

        const session = await client.openSession({
                    client_protocol: TCLIService_types.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10
                });
    
    
                const data = await session.executeStatement(
                            'select * from json1', { runAsync: true }
                        );
                    
                        await utils.waitUntilReady(data, false, () => {});
                        await utils.fetchAll(data);
                        await data.close()
                    
                        const result = utils.getResult(data).getValue()
                    
                        
                    
                        res.send(JSON.stringify(result));
                        await session.close();
    
    }).catch(err => console.log(err))
  
});

app.listen(PORT, HOST, () => {
  console.log(`Running on http://${HOST}:${PORT}`);
});