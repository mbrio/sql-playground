winston = require 'winston'
fs = require 'fs'
path = require 'path'
mssql = require 'mssql'
Promise = require 'bluebird'
glob = require 'glob'
config = require('confurg').init
  cwd: __dirname
  namespace: 'sql-playground'

maxLogCount = config.maxLogCount ? 10
maxConnectionTries = config.maxConnectionTries ? 10
fileName = config.fileName ? 'request.sql'
logName = config.logName ? 'out.tsv'

console.log config

logExt = path.extname logName
baseLogName = path.basename logName, logExt

filePath = path.join __dirname, fileName
logPath = path.join __dirname, 'data', logName

connectionTries = 0
currentRequest = null

process.on 'SIGINT', ->
  if currentRequest then currentRequest.cancel()
  mssql.close()
  process.exit 0

logger =
  reset: ->
    if fs.existsSync(logPath)
      if fs.readFileSync(logPath).toString().trim().length > 0
        newLogName = "#{baseLogName}-#{new Date().getTime()}#{logExt}"
        newLogPath = path.join __dirname, 'data', newLogName
        fs.renameSync logPath, newLogPath

    outFiles = glob.sync("data/#{baseLogName}-*#{logExt}").sort (a, b) ->
      if a > b then -1
      else if a < b then 1
      else 0

    if outFiles.length > maxLogCount
      filesToRemove = outFiles.splice maxLogCount
      fs.unlinkSync(path.join(__dirname, f)) for f in filesToRemove

    fs.writeFileSync logPath, ''
  info: (msg) ->
    console.log msg
  data: (msg, isHeader=false) ->
    unless isHeader
      fs.appendFileSync logPath, msg
      fs.appendFileSync logPath, '\n'
    console.log msg
  error: (msg) ->
    console.error msg

makeRequest = ->
  new Promise (resolve, reject) ->
    mssql.connect config.sql, ->
      connectionTries = 0
      currentRequest = request = new mssql.Request()
      request.stream = true

      request.on 'recordset', (columns) ->
        logger.data (key for key, val of columns).join('\t'), true

      request.on 'row', (row) ->
        logger.data (val for key, val of row).join '\t'

      request.on 'error', (err) ->
        currentRequest = null
        request.cancel()
        reject err

      request.on 'done', (returnValue) ->
        currentRequest = null
        resolve returnValue

      request.query fs.readFileSync(filePath).toString()

sqlCommand = ->
  logger.reset()

  makeRequest()
    .catch (err) ->
      if err.name is 'ConnectionError'
        if connectionTries <= maxConnectionTries
          logger.error 'Connection Error, retrying...'
          connectionTries++
          sqlCommand()
        else
          logger.error 'Too many connection errors, no longer retrying.'
      else
        logger.error err
    .finally -> mssql.close()

sqlCommand.fileName = fileName

module.exports = sqlCommand
