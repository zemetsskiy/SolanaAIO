const fs = require('fs');
const path = require('path');
const toml = require('toml');
const bs58 = require('bs58');
const { Connection, Keypair, LAMPORTS_PER_SOL, Transaction, SystemProgram, SendTransactionError } = require('@solana/web3.js');
const winston = require('winston');

const configPath = path.join(__dirname, 'config', 'settings.toml');
const configData = fs.readFileSync(configPath, 'utf-8');
const config = toml.parse(configData);

const RPC_ENDPOINT = config.rpc_endpoint;
const TRANSACTION_FEE_SOL = config.transaction_fee_sol || 0.001;
const CONFIRMATION_TIMEOUT_SECONDS = config.confirmation_timeout_seconds || 60;
const KEYS_FILE_PATH = path.resolve(__dirname, config.keys_file_path);
const LOGS_DIR_PATH = path.resolve(__dirname, config.logs_dir_path);
const TRANSACTIONS_LOG_FILE = path.resolve(__dirname, config.transactions_log_file);
const ERRORS_LOG_FILE = path.resolve(__dirname, config.errors_log_file);

if (!fs.existsSync(LOGS_DIR_PATH)) {
    fs.mkdirSync(LOGS_DIR_PATH);
}

const logFormat = winston.format.printf(({ level, message, timestamp }) => {
    return `${level.toUpperCase()} - ${timestamp} - ${message}`;
});

const transactionLogger = winston.createLogger({
    level: 'info',
    format: winston.format.combine(
        winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
        logFormat
    ),
    transports: [
        new winston.transports.File({ filename: TRANSACTIONS_LOG_FILE })
    ]
});

const errorLogger = winston.createLogger({
    level: 'error',
    format: winston.format.combine(
        winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
        logFormat
    ),
    transports: [
        new winston.transports.File({ filename: ERRORS_LOG_FILE })
    ]
});

function readPrivateKeys(filePath) {
    const data = fs.readFileSync(filePath, 'utf-8');
    const keys = data.split(/\r?\n/).filter(line => line.trim() !== '');
    return keys;
}

function parsePrivateKey(line) {
    line = line.trim();
    if (line.startsWith('[') && line.endsWith(']')) {
        const arrayString = line.slice(1, -1);
        const byteArray = arrayString.split(',').map(num => parseInt(num.trim(), 10));
        return Uint8Array.from(byteArray);
    } else {
        try {
            return bs58.decode(line);
        } catch (err) {
            throw new Error('Invalid Base58 format');
        }
    }
}

function maskAddress(address) {
    if (address.length <= 10) return address;
    return `${address.slice(0,5)}...${address.slice(-5)}`;
}

async function confirmTransactionWithTimeout(connection, signature, commitment, timeoutSeconds) {
    return new Promise((resolve, reject) => {
        const timeout = setTimeout(() => {
            reject(new Error(`Transaction was not confirmed in ${timeoutSeconds} seconds.`));
        }, timeoutSeconds * 1000);

        connection.confirmTransaction(signature, commitment).then(result => {
            clearTimeout(timeout);
            resolve(result);
        }).catch(err => {
            clearTimeout(timeout);
            reject(err);
        });
    });
}

(async () => {
    try {
        const connection = new Connection(RPC_ENDPOINT, 'confirmed');
        const privateKeys = readPrivateKeys(KEYS_FILE_PATH);

        if (privateKeys.length < 2) {
            console.error('At least two wallets are required.');
            process.exit(1);
        }

        let recipientKeypair;
        try {
            const recipientSecretKey = parsePrivateKey(privateKeys[0]);
            recipientKeypair = Keypair.fromSecretKey(recipientSecretKey);
            console.log(`Recipient Wallet: ${maskAddress(recipientKeypair.publicKey.toBase58())}`);
        } catch (err) {
            errorLogger.error(`Recipient key parsing error: ${err.message}`);
            console.error('Failed to parse the recipient private key. Check errors.log for details.');
            process.exit(1);
        }

        let actionCount = 0;

        for (let i = 1; i < privateKeys.length; i++) {
            const line = privateKeys[i];
            let senderKeypair;
            try {
                const secretKey = parsePrivateKey(line);
                senderKeypair = Keypair.fromSecretKey(secretKey);
                console.log(`Processing Wallet ${i}: ${maskAddress(senderKeypair.publicKey.toBase58())}`);
            } catch (err) {
                errorLogger.error(`Iteration ${i}: Invalid private key format.`);
                console.error(`Iteration ${i}: Invalid private key format. Check errors.log for details.`);
                continue;
            }

            try {
                const senderPublicKey = senderKeypair.publicKey;
                const recipientPublicKey = recipientKeypair.publicKey;

                const senderBalanceLamports = await connection.getBalance(senderPublicKey);
                const minimumBalance = await connection.getMinimumBalanceForRentExemption(0);
                const transactionFeeLamports = TRANSACTION_FEE_SOL * LAMPORTS_PER_SOL;
                const amountToSend = senderBalanceLamports - minimumBalance - transactionFeeLamports;

                console.log(`Wallet ${i}: Balance = ${(senderBalanceLamports / LAMPORTS_PER_SOL).toFixed(6)} SOL`);

                if (amountToSend <= 0) {
                    transactionLogger.info(`Iteration ${i}: Insufficient balance for wallet ${maskAddress(senderPublicKey.toBase58())}.`);
                    continue;
                }

                const transaction = new Transaction().add(
                    SystemProgram.transfer({
                        fromPubkey: senderPublicKey,
                        toPubkey: recipientPublicKey,
                        lamports: amountToSend,
                    })
                );

                const signature = await connection.sendTransaction(transaction, [senderKeypair], {
                    skipPreflight: false,
                    preflightCommitment: 'confirmed',
                });

                await confirmTransactionWithTimeout(connection, signature, 'confirmed', CONFIRMATION_TIMEOUT_SECONDS);

                transactionLogger.info(`Sent ${(amountToSend / LAMPORTS_PER_SOL).toFixed(6)} SOL from ${maskAddress(senderPublicKey.toBase58())} to ${maskAddress(recipientPublicKey.toBase58())}. Transaction: ${signature}`);

                actionCount++;
                console.log(`Actions completed: ${actionCount}`);
            } catch (err) {
                let logs = [];
                if (err instanceof SendTransactionError && err.transaction) {
                    try {
                        const tx = await connection.getTransaction(err.transaction, { commitment: 'confirmed' });
                        logs = tx?.meta?.logMessages || [];
                    } catch {}
                }
                errorLogger.error(`Iteration ${i}: Transfer error - ${err.message}. Logs: ${JSON.stringify(logs)}`);
                console.error(`Iteration ${i}: Transfer error. Check errors.log for details.`);
            }
        }

        console.log(`Finished. Total actions completed: ${actionCount}`);
    } catch (err) {
        errorLogger.error(`General error: ${err.message}`);
        console.error('An error occurred. Check logs for details.');
    }
})();
