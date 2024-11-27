const { Connection, PublicKey } = require('@solana/web3.js');
const fs = require('fs').promises;
const path = require('path');
const toml = require('toml');
const winston = require('winston');

const loadConfig = async (configPath) => {
    const configContent = await fs.readFile(configPath, 'utf-8');
    return toml.parse(configContent);
};

const setupLogging = (logsDir, transactionsLogFile, errorsLogFile) => {
    fs.mkdir(logsDir, { recursive: true }).catch(err => {
        console.error('Error creating logs directory:', err);
        process.exit(1);
    });

    const transactionLogger = winston.createLogger({
        level: 'info',
        format: winston.format.json(),
        transports: [
            new winston.transports.File({ filename: transactionsLogFile })
        ],
    });

    const errorLogger = winston.createLogger({
        level: 'error',
        format: winston.format.json(),
        transports: [
            new winston.transports.File({ filename: errorsLogFile })
        ],
    });

    return { transactionLogger, errorLogger };
};

const withRetry = async (fn, retries = 5, initialDelay = 500) => {
    let attempt = 0;
    let currentDelay = initialDelay;
    while (attempt < retries) {
        try {
            return await fn();
        } catch (error) {
            if (error.message.includes('429')) {
                console.warn(`Error 429 Too Many Requests. Retrying in ${currentDelay}ms...`);
                await new Promise(res => setTimeout(res, currentDelay));
                currentDelay *= 2;
                attempt++;
            } else {
                throw error;
            }
        }
    }
    throw new Error('Maximum retry attempts reached');
};

const main = async () => {
    const configPath = path.join(__dirname, 'config', 'settings.toml');
    const config = await loadConfig(configPath);

    const {
        rpc_endpoint,
        logs_dir_path,
        transactions_log_file,
        errors_log_file,
        wallets_file_path,
        transaction_count_range,
        min_balance_sol,
        max_concurrent_requests
    } = config;

    const { transactionLogger, errorLogger } = setupLogging(logs_dir_path, transactions_log_file, errors_log_file);

    const connection = new Connection(rpc_endpoint, {
        commitment: 'confirmed',
        disableRetryOnRateLimit: true,
    });

    let walletList;
    try {
        const data = await fs.readFile(wallets_file_path, 'utf-8');
        walletList = data.split('\n').map(line => line.trim()).filter(line => line);
    } catch (error) {
        console.error('Error reading wallets file:', error);
        process.exit(1);
    }

    const minBalanceLamports = min_balance_sol * 1e9;
    const [txMin, txMax] = transaction_count_range;

    console.log(`Filtering wallets with balance >= ${min_balance_sol} SOL and transaction count between ${txMin} and ${txMax}...`);

    const checkWallet = async (address) => {
        try {
            const publicKey = new PublicKey(address);

            const balanceLamports = await withRetry(() => connection.getBalance(publicKey));
            if (balanceLamports < minBalanceLamports) {
                return null;
            }

            const signatures = await withRetry(() => connection.getSignaturesForAddress(publicKey, { limit: 1000 }));
            const transactionCount = signatures.length;

            if (transactionCount < txMin || transactionCount > txMax) {
                return null;
            }

            const walletInfo = {
                address,
                balance: balanceLamports / 1e9,
                transactionCount
            };

            transactionLogger.info(walletInfo);
            return walletInfo;
        } catch (error) {
            errorLogger.error({ address, error: error.message });
            console.error(`Error processing wallet ${address}:`, error.message);
            return null;
        }
    };

    const asyncPool = async (poolLimit, array, iteratorFn) => {
        const ret = [];
        const executing = [];
        for (const item of array) {
            const p = Promise.resolve().then(() => iteratorFn(item));
            ret.push(p);

            if (poolLimit <= array.length) {
                const e = p.then(() => executing.splice(executing.indexOf(e), 1));
                executing.push(e);
                if (executing.length >= poolLimit) {
                    await Promise.race(executing);
                }
            }
        }
        return Promise.all(ret);
    };

    try {
        const results = await asyncPool(
            max_concurrent_requests,
            walletList,
            checkWallet
        );

        const filteredWallets = results.filter(wallet => wallet !== null);
        console.log(`Found ${filteredWallets.length} wallets matching the criteria.`);

        const outputFilePath = path.join(logs_dir_path, 'filteredWallets.json');
        await fs.writeFile(outputFilePath, JSON.stringify(filteredWallets, null, 2));
        console.log(`Results saved to ${outputFilePath}`);
    } catch (error) {
        errorLogger.error({ error: error.message });
        console.error('Error in main process:', error);
    }
};

main();
