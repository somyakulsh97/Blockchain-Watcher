import mysql.connector
from mysql.connector import Error
from web3 import Web3


infura_url = "https://kovan.infura.io/v3/2211055978ce412d8df670319d322f5a"
web3 = Web3(Web3.HTTPProvider(infura_url))
print(web3.isConnected())

def get_accounts():
    connection = connect_db()
    cursor = connection.cursor()
    query = "SELECT Address from Addresses"
    cursor.execute(query)
    addresses = cursor.fetchall()
    return addresses

def check_block():
    accounts = get_accounts()
    latest_block = web3.eth.get_block('pending')
    print(latest_block.number)
    for transaction in latest_block.transactions:
        transaction_hash = transaction.hex()
        transaction_block = web3.eth.get_transaction(transaction_hash)
        for address in accounts:
            if(transaction_block.to == address[0]):
                block_hash = latest_block.hash.hex()
                parent_hash = latest_block.parentHash.hex()
                timestamp = latest_block.timestamp

                from_address = transaction_block['from']
                to_address = transaction_block.to
                amount = transaction_block.value
                insert_data(block_hash, parent_hash, timestamp, transaction_hash, from_address, to_address, amount)
                print("Data inserted")


def connect_db():
    try:
        connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password='omnamahshivay',
            database='ethdb'
        )
        if connection.is_connected():
            #print('database is connected')
            return connection

    except Error as e:
        print(e)

def insert_data(block_hash,parent_hash,timestamp,transaction_hash,from_address,to_address,amount):
    connection = connect_db()
    cursor = connection.cursor()
    block_sql = "INSERT INTO Blocks (Block_Hash,Parent_Hash,Time_Stamp) VALUES (%s,%s,%s)"
    block_values = (block_hash,parent_hash,timestamp)
    cursor.execute(block_sql, block_values)
    trans_sql = "INSERT INTO Transactions (Transaction_Hash,From_Address,To_Address,Time_Stamp,Amount) VALUES (%s,%s,%s,%s,%s)"
    trans_values = (transaction_hash,from_address,to_address,timestamp,amount)
    cursor.execute(trans_sql, trans_values)
    connection.commit()
    cursor.close()
    connection.close()

check_block()




