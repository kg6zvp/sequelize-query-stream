import QueryStream from 'pg-query-stream'
import pump from 'pump'
import through2 from 'through2'

export const select = (pg, v, tableName, model) =>
  pg.dialect.QueryGenerator.selectQuery(tableName, v, model)

// this wraps a sql query in a stream via a cursor so as each row is found
// it gets transformed and emitted from the stream
// this is how you want to return millions of rows with low memory overhead
const batchSize = 16
const streamable = async (pg, sql) => {
  const conn = await pg.connectionManager.getConnection({
    type: 'SELECT'
  })
  // a not so fun hack to tie our sequelize types into this raw cursor
  const query = conn.query(new QueryStream(sql, undefined, {
    batchSize,
    types: {
      getTypeParser: conn.getTypeParser.bind(conn)
    }
  }))

  const modifier = through2.obj(({ data }, _, cb) =>
    cb(null, data)
  )

  const end = (err) => {
    query.close(() => {
      pg.connectionManager.releaseConnection(conn)
        .then(() => null)
        .catch(() => null)
    })
    if (err) out.emit('error', err)
  }
  const out = pump(query, modifier, end)
  return out
}

const createStream = async ({ pg, Model, query, scope }) => {
  const model = Model.scope(scope)
  model._expandAttributes(query)
  const sql = select(query, Model.getTableName(), model)
  return await streamable(pg, sql)
}

export default createStream
