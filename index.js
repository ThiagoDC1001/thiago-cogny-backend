const { DATABASE_SCHEMA, DATABASE_URL, SHOW_PG_MONITOR } = require('./config');
const massive = require('massive');
const monitor = require('pg-monitor');
const axios = require('axios');

// Call start
(async () => {
    console.log('main.js: before start');    

const url = 'https://datausa.io/api/data?drilldowns=Nation&measures=Population'


    const getData = async () => {
        const response = await axios.get(url)        
        return response.data
    }


    const db = await massive({
        connectionString: DATABASE_URL,
        ssl: { rejectUnauthorized: false },
    }, {
        // Massive Configuration
        scripts: process.cwd() + '/migration',
        allowedSchemas: [DATABASE_SCHEMA],
        whitelist: [`${DATABASE_SCHEMA}.%`],
        excludeFunctions: true,
    }, {
        // Driver Configuration
        noWarnings: true,
        error: function (err, client) {
            console.log(err);
            //process.emit('uncaughtException', err);
            //throw err;
        }
    });

    if (!monitor.isAttached() && SHOW_PG_MONITOR === 'true') {
        monitor.attach(db.driverConfig);
    }

    const execFileSql = async (schema, type) => {
        return new Promise(async resolve => {
            const objects = db['user'][type];

            if (objects) {
                for (const [key, func] of Object.entries(objects)) {
                    console.log(`executing ${schema} ${type} ${key}...`);
                    await func({
                        schema: DATABASE_SCHEMA,
                    });
                }
            }

            resolve();
        });
    };

    //public
    const migrationUp = async () => {
        return new Promise(async resolve => {
            await execFileSql(DATABASE_SCHEMA, 'schema');

            //cria as estruturas necessarias no db (schema)
            await execFileSql(DATABASE_SCHEMA, 'table');
            await execFileSql(DATABASE_SCHEMA, 'view');

            console.log(`reload schemas ...`)
            await db.reload();

            resolve();
        });
    };

    try {
        await migrationUp();

        //exemplo de insert
        const data = await getData();        
        const result1 = db[DATABASE_SCHEMA].api_data.insert({doc_record: JSON.stringify(data)}, function (err, result) {            
        })
        console.log('result1 >>>', result1);

        const totalPopulation = data.data.filter(data=> data.Year >= 2018 && data.Year <= 2020).map((x) => x.Population).reduce((acc, val) => acc + val)
        console.log ("RESULTADO EM MEMÓRIA>>>", totalPopulation)

        //exemplo select
        const viewResult = await db[DATABASE_SCHEMA].vw_population.findOne({}, {
            fields: ['population']
        });
        console.log('Resultado da view >>>', viewResult);

    } catch (e) {
        console.log(e.message)
    } finally {
        console.log('finally');
    }
    console.log('main.js: after start');
})();