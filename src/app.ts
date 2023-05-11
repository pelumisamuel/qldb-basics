import { Agent } from 'https'
import { NodeHttpHandlerOptions } from '@aws-sdk/node-http-handler'
import { QLDBSessionClientConfig } from '@aws-sdk/client-qldb-session'
import {
  QldbDriver,
  RetryConfig,
  TransactionExecutor,
} from 'amazon-qldb-driver-nodejs'
import {
  CreateLedgerRequest,
  CreateLedgerResponse,
  DescribeLedgerRequest,
  DescribeLedgerResponse,
  QLDB,
} from '@aws-sdk/client-qldb'
import { dom } from 'ion-js'

async function main(): Promise<void> {
  const ledgerName: string = 'community-journal'
  const maxConcurrentTransactions: number = 10
  // setting connection pools
  const agentForQldb: Agent = new Agent({
    maxSockets: maxConcurrentTransactions,
  })

  const lowLevelClientHttpOptions: NodeHttpHandlerOptions = {
    httpAgent: agentForQldb,
  }
  const serviceConfigurationOptions: QLDBSessionClientConfig = {
    region: 'us-east-1',
  }
  const retryLimit: number = 4
  const retryConfig: RetryConfig = new RetryConfig(retryLimit)
  const driver: QldbDriver = new QldbDriver(
    ledgerName,
    serviceConfigurationOptions,
    lowLevelClientHttpOptions,
    maxConcurrentTransactions,
    retryConfig
  )
  try {
    // creating ledger first
    const qldbClient: QLDB = new QLDB({})
    await createLedger(ledgerName, qldbClient)
    await waitForActive(ledgerName, qldbClient)

    const peopleList = await driver.executeLambda(
      async (txn: TransactionExecutor) => {
        console.log('Creating table People')
        // await createTable(txn)
        console.log('creating index on the first name')
        // await createIndex(txn)
        console.log('Insert document')
        // await insertDocument(txn)
        await updateDocuments(txn)
        console.log('fetch documents')
        return await fetchDocuments(txn)
      }
    )
    console.log('this is a people list', JSON.stringify(peopleList, null, 2))

    driver.close()
  } catch (error) {
    throw new Error(`something went wrong ${error}`)
  }
}

if (require.main === module) {
  main()
}

// create a ledger first
async function createLedger(
  ledgerName: string,
  qldbClient: QLDB
): Promise<CreateLedgerResponse | void> {
  console.log(`creating ledger ${ledgerName}`)
  const request: CreateLedgerRequest = {
    Name: ledgerName,
    PermissionsMode: 'ALLOW_ALL',
  }
  const resp = await qldbClient.describeLedger(request)
  if (resp.State !== 'ACTIVE') {
    const result: CreateLedgerResponse = await qldbClient.createLedger(request)
    console.log(`Success. Ledger state: ${result.State}.`)
    return result
  }
}

// check whether the legder is ready..
async function waitForActive(
  ledgerName: string,
  qldbClient: QLDB
): Promise<DescribeLedgerResponse> {
  console.log(`Waiting for ledger ${ledgerName} to be ready and active...`)
  const request: DescribeLedgerRequest = {
    Name: ledgerName,
  }
  while (true) {
    let loopCount: number = 0
    const result: DescribeLedgerResponse = await qldbClient.describeLedger(
      request
    )

    if (result.State === 'ACTIVE') {
      return result
    }
    console.log('ledger is not ready yet. wait...')
    await new Promise((resolve) => setTimeout(resolve, 10000))

    if (loopCount > 30) {
      console.log('creating ledger timeout error')
      return result
    }
    loopCount++
    console.log(loopCount, 'loop count>>>>>>>>>>>>>>>')
  }
}

// create table in the ledger
async function createTable(txn: TransactionExecutor) {
  await txn.execute('CREATE TABLE People')
}

// index the first name in the ledger
async function createIndex(txn: TransactionExecutor) {
  await txn.execute('CREATE INDEX ON People (firstName)')
}

// insert a new document
async function insertDocument(txn: TransactionExecutor): Promise<void> {
  const person: Record<string, any> = {
    firstName: 'John',
    lastName: 'Doe',
    age: 42,
  }
  let res = await txn.execute('INSERT INTO People ?', person)
  console.log(res)
}

async function fetchDocuments(txn: TransactionExecutor): Promise<dom.Value[]> {
  let firstName = 'John'
  const result = (
    await txn.execute(
      'SELECT firstName, age, lastName from People where firstName = ?',
      firstName
    )
  ).getResultList()

  return result
}

async function updateDocuments(txn: TransactionExecutor): Promise<void> {
  const lastName = 'Stiles'
  await txn.execute(
    'UPDATE People SET lastName = ? WHERE firstName = ?',
    lastName,
    'John'
  )
}
