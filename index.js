const fs = require('fs')
const csv2json = require('csvtojson')
const { Transform, pipeline } = require('stream')
const { createGzip } = require('zlib')
const bufferingObjectStream = require('buffering-object-stream')


const main = async () => {
    let startTime = performance.now()
    const readStream = fs.createReadStream('./data/import.csv')
    const writeStream = fs.createWriteStream('./data/export.ndjson')

    const simpleTransform = new Transform({
        objectMode: true,
        transform(chunk, enc, callback) {
            const user = {
                ...chunk,
                email: chunk.email.toLowerCase(),
                isActive: chunk.isActive === "true",
                isEligible: Number(chunk.age) >= 18 ? true: false
            }
            callback(null, user)
        }
    })

    const filterTransform = new Transform({
        objectMode: true,
        transform(users, enc, callback) {
            const activeUsers = users.filter((user) => user.isActive)
            callback(null, activeUsers)

            // if(!user.isActive){
            //     callback(null)
            //     return
            // }
            // callback(null, user)
        }
    })

    const convertToNdjson = new Transform({
        objectMode: true,
        transform(user, enc, callback) {
            const value = JSON.stringify(user) + '\n'
            callback(null, value)
        }
    })
    // readStream
    //     .pipe(csv2json({
    //         delimiter: ';'
    //     }, {
    //         objectMode: true
    //     }))
    //     .pipe(simpleTransform)
    //     .pipe(filterTransform)
    //     .on('data', data => {
    //         console.log(">>>>>")
    //         console.log(data)
    //     })
    
    pipeline(
        readStream,
        csv2json({ delimiter: ';'}, { objectMode: true }),
        simpleTransform,
        bufferingObjectStream(10),
        filterTransform,
        convertToNdjson,
        createGzip(),
        fs.createWriteStream('./data/export.ndjson.gz'),
        (err) => {
            if (err) {
              console.error('Pipeline failed.', err);
            } else {
              console.log('Pipeline succeeded.');
            }
        },
    )

    let endTime = performance.now()

    console.log(`It took ${endTime - startTime} milliseconds`)
}

main()