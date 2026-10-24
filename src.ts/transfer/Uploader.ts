import {
    DEFAULT_SEGMENT_SIZE,
    DEFAULT_SEGMENT_MAX_CHUNKS,
    DEFAULT_CHUNK_SIZE,
    DEFAULT_BATCH_SIZE,
} from '../constant.js'
import { StorageNode, SegmentWithProof, FileInfo } from '../node/index.js'
import {
    FixedPriceFlow,
    SubmissionStruct,
    SubmitEvent,
} from '../contracts/flow/FixedPriceFlow.js'
import { delay, getMarketContract, SegmentRange } from '../utils.js'
import { RetryOpts } from '../types.js'
import { MerkleTree, nextPow2 } from '../file/index.js'
import { encodeBase64, ethers } from 'ethers'
import { calculatePrice, getShardConfigs } from './utils.js'
import {
    EncryptionOption,
    UploadOption,
    UploadTask,
    mergeUploadOptions,
} from './types.js'
import { AbstractFile } from '../file/AbstractFile.js'
import {
    newEciesEncryptedFile,
    newSymmetricEncryptedFile,
} from '../file/EncryptedFile.js'
import { checkReplica, ShardConfig } from '../common/index.js'

export class Uploader {
    nodes: StorageNode[]
    provider: ethers.JsonRpcProvider
    flow: FixedPriceFlow
    gasPrice: bigint
    gasLimit: bigint

    constructor(
        nodes: StorageNode[],
        providerRpc: string,
        flow: FixedPriceFlow,
        gasPrice: bigint = BigInt('0'),
        gasLimit: bigint = BigInt('0')
    ) {
        this.nodes = nodes

        this.provider = new ethers.JsonRpcProvider(providerRpc)

        this.flow = flow

        this.gasPrice = gasPrice
        this.gasLimit = gasLimit
    }

    async uploadFile(
        file: AbstractFile,
        opts: UploadOption,
        retryOpts?: RetryOpts
    ): Promise<
        [{ txHash: string; rootHash: string; txSeq: number }, Error | null]
    > {
        const mergedOpts = mergeUploadOptions(opts)
        if (mergedOpts.encryption) {
            file = this.wrapEncryption(file, mergedOpts.encryption)
        }
        var [tree, err] = await file.merkleTree()
        if (err != null || tree == null || tree.rootHash() == null) {
            return [
                { txHash: '', rootHash: '', txSeq: 0 },
                new Error('Failed to create Merkle tree, ' + err),
            ]
        }

        const rootHash = tree.rootHash() as string

        console.log(
            'Data prepared to upload',
            'root=' + rootHash,
            'size=' + file.size(),
            'numSegments=' + file.numSegmentsPadded(),
            'numChunks=' + file.numChunks()
        )

        const onProgress = mergedOpts.onProgress
        let txHash = ''
        let info = await this.findExistingFileInfo(rootHash)

        if (mergedOpts.skipIfFinalized && info !== null && info.finalized) {
            onProgress?.('File already stored on network.')
            return [{ txHash: '', rootHash, txSeq: info.tx.seq }, null]
        }

        if (!mergedOpts.skipTx || info === null) {
            const submitter =
                mergedOpts.submitter && mergedOpts.submitter.length > 0
                    ? mergedOpts.submitter
                    : await this.resolveSubmitter()
            var [submission, err] = await file.createSubmission(
                mergedOpts.tags!,
                submitter
            )
            if (err !== null || submission === null) {
                return [
                    { txHash: '', rootHash, txSeq: 0 },
                    new Error('Failed to create submission'),
                ]
            }

            // Submit TX without waiting for receipt — matches Go's submitLogEntryNoReceipt
            const [submittedTxHash, txErr] = await this.submitLogEntryNoReceipt(
                submission,
                opts
            )
            if (txErr !== null) {
                return [{ txHash: '', rootHash, txSeq: 0 }, txErr]
            }
            txHash = submittedTxHash
            onProgress?.(`Transaction submitted: ${txHash}`)

            // Wait for log entry by root hash (no txSeq needed) — matches Go's uploadSlowParallel
            info = await this.waitForLogEntry(
                rootHash,
                false,
                0,
                false,
                onProgress
            )
        }

        if (info === null) {
            return [
                { txHash, rootHash, txSeq: 0 },
                new Error('Failed to get log entry'),
            ]
        }

        const txSeq = info.tx.seq
        onProgress?.(
            `Log entry confirmed (txSeq=${txSeq}). Uploading segments...`
        )

        const tasks = await this.splitTasks(info, tree, opts)
        if (tasks === null) {
            return [
                { txHash, rootHash, txSeq },
                new Error('Failed to get upload tasks'),
            ]
        }

        if (tasks.length === 0) {
            onProgress?.('File already stored on network.')
            return [{ txHash, rootHash, txSeq }, null]
        }

        console.log(
            'Processing tasks in parallel with ',
            tasks.length,
            ' tasks...'
        )

        const results = await this.processTasksInParallel(
            file,
            tree,
            tasks,
            retryOpts
        )

        // Check if any task failed
        for (let i = 0; i < results.length; i++) {
            if (results[i] instanceof Error) {
                return [{ txHash, rootHash, txSeq }, results[i] as Error]
            }
        }

        console.log('All tasks processed')
        onProgress?.('Segments uploaded. Waiting for finality...')

        await this.waitForLogEntry(
            rootHash,
            mergedOpts.finalityRequired,
            info.tx.seq,
            false,
            onProgress
        )
        onProgress?.('Upload finalized.')

        return [{ txHash, rootHash, txSeq }, null]
    }

    /**
     * Upload file with automatic splitting into fragments if it exceeds fragmentSize
     * @param file File to upload
     * @param fragmentSize Maximum size of each fragment
     * @param opts Upload options
     * @param retryOpts Retry options
     * @returns Promise resolving to arrays of transaction hashes and root hashes
     */
    async splitableUpload(
        file: AbstractFile,
        opts: UploadOption = {},
        retryOpts?: RetryOpts
    ): Promise<
        [
            { txHashes: string[]; rootHashes: string[]; txSeqs: number[] },
            Error | null
        ]
    > {
        const mergedOpts = mergeUploadOptions(opts)

        // Check fragment size
        if (mergedOpts.fragmentSize < DEFAULT_CHUNK_SIZE) {
            mergedOpts.fragmentSize = DEFAULT_CHUNK_SIZE
        }

        // Align size of fragment to 2 power

        mergedOpts.fragmentSize = nextPow2(mergedOpts.fragmentSize)

        const txHashes: string[] = []
        const rootHashes: string[] = []
        const txSeqs: number[] = []

        if (file.size() <= mergedOpts.fragmentSize) {
            // Single file upload
            const [result, err] = await this.uploadFile(
                file,
                mergedOpts,
                retryOpts
            )
            if (err != null) {
                return [{ txHashes, rootHashes, txSeqs }, err]
            }
            txHashes.push(result.txHash)
            rootHashes.push(result.rootHash)
            txSeqs.push(result.txSeq)
        } else {
            // Split and batch upload
            const fragments = file.split(mergedOpts.fragmentSize)
            console.log(
                `Splitted origin file into ${fragments.length} fragments, ${mergedOpts.fragmentSize} bytes each.`
            )

            for (let l = 0; l < fragments.length; l += DEFAULT_BATCH_SIZE) {
                const r = Math.min(l + DEFAULT_BATCH_SIZE, fragments.length)
                console.log(`Batch uploading fragments ${l} to ${r}...`)

                // Process fragments sequentially to maintain order
                for (let i = l; i < r; i++) {
                    const [result, err] = await this.uploadFile(
                        fragments[i],
                        mergedOpts,
                        retryOpts
                    )
                    if (err != null) {
                        return [{ txHashes, rootHashes, txSeqs }, err]
                    }
                    txHashes.push(result.txHash)
                    rootHashes.push(result.rootHash)
                    txSeqs.push(result.txSeq)
                }
            }
        }

        return [{ txHashes, rootHashes, txSeqs }, null]
    }

    // submitLogEntryNoReceipt submits the TX and returns the txHash immediately
    // without waiting for receipt — matches Go's submitLogEntryNoReceipt.
    private wrapEncryption(
        file: AbstractFile,
        enc: EncryptionOption
    ): AbstractFile {
        switch (enc.type) {
            case 'aes256':
                return newSymmetricEncryptedFile(file, enc.key)
            case 'ecies':
                return newEciesEncryptedFile(file, enc.recipientPubKey)
        }
    }

    private async submitLogEntryNoReceipt(
        submission: SubmissionStruct,
        opts: UploadOption = {}
    ): Promise<[string, Error | null]> {
        const mergedOpts = mergeUploadOptions(opts)
        const marketAddr = await this.flow.market()
        const marketContract = getMarketContract(marketAddr, this.provider)
        const pricePerSector = await marketContract.pricePerSector()

        let fee: bigint = BigInt('0')
        if (mergedOpts.fee > 0) {
            fee = mergedOpts.fee
        } else {
            fee = calculatePrice(submission, pricePerSector)
        }

        const txOpts: {
            value: bigint
            gasPrice?: bigint
            gasLimit?: bigint
            nonce?: number
        } = {
            value: fee,
            nonce:
                mergedOpts.nonce !== undefined
                    ? Number(mergedOpts.nonce)
                    : undefined,
        }

        if (this.gasPrice > 0) {
            txOpts.gasPrice = this.gasPrice
        } else {
            const suggestedGasPrice = (await this.provider.getFeeData())
                .gasPrice
            if (suggestedGasPrice === null) {
                return [
                    '',
                    new Error(
                        'Failed to get suggested gas price, set your own gas price'
                    ),
                ]
            }
            txOpts.gasPrice = suggestedGasPrice
        }

        if (this.gasLimit > 0) {
            txOpts.gasLimit = this.gasLimit
        }

        console.log('Submitting transaction with storage fee:', fee)

        try {
            const resp = await this.flow
                .getFunction('submit')
                .send(submission, txOpts)
            console.log('Transaction submitted, hash:', resp.hash)
            return [resp.hash, null]
        } catch (e) {
            return ['', e as Error]
        }
    }

    private async resolveSubmitter(): Promise<string> {
        const runner = this.flow.runner as {
            getAddress?: () => Promise<string>
        } | null
        if (runner?.getAddress) {
            return await runner.getAddress()
        }
        return ethers.ZeroAddress
    }

    private async findExistingFileInfo(
        rootHash: string
    ): Promise<FileInfo | null> {
        console.log('Attempting to find existing file info by root hash...')

        // Try to get file info from storage nodes
        for (let client of this.nodes) {
            try {
                const info = await client.getFileInfo(rootHash, false)
                if (info !== null) {
                    console.log('Found existing file info:', info)
                    return info
                }
            } catch (e) {
                console.log('Failed to get file info from node:', client.url)
            }
        }

        return null
    }

    async processLogs(receipt: ethers.TransactionReceipt): Promise<number[]> {
        const contractAddress = (await this.flow.getAddress()).toLowerCase()
        const signature = this.flow.interface.getEvent('Submit')
        var txSeqs: number[] = []
        for (const log of receipt.logs) {
            // Only process logs that are emitted by this contract.
            if (log.address.toLowerCase() !== contractAddress) {
                continue
            }
            if (log.topics[0] !== signature.topicHash) {
                continue
            }

            try {
                // Use the contract's interface to parse the log.
                const parsedLog = this.flow.interface.parseLog(log)
                if (!parsedLog) {
                    continue
                }

                // Check if the event name is "Submit"
                if (parsedLog.name === 'Submit') {
                    const event =
                        parsedLog.args as unknown as SubmitEvent.OutputObject
                    txSeqs.push(Number(event.submissionIndex))
                }
            } catch (error) {
                // If parseLog fails, this log is not one of our events.
                // You can log the error if needed.
                // console.error("Error decoding log:", error);
                continue
            }
        }

        return txSeqs
    }

    async waitForReceipt(
        txHash: string,
        opts?: RetryOpts
    ): Promise<ethers.TransactionReceipt | null> {
        var receipt: ethers.TransactionReceipt | null = null

        if (opts === undefined) {
            opts = { Retries: 10, Interval: 5, MaxGasPrice: 0 }
        }

        let nTries = 0

        while (nTries < opts.Retries) {
            receipt = await this.provider.getTransactionReceipt(txHash)
            if (receipt !== null && receipt.status == 1) {
                return receipt
            }
            await delay(opts.Interval * 1000)
            nTries++
        }

        return null
    }

    async waitForLogEntry(
        root: string,
        finalityRequired: boolean,
        txSeq: number,
        useTxSeq: boolean,
        onProgress?: (message: string) => void
    ): Promise<FileInfo | null> {
        console.log('Wait for log entry on storage node')

        let info: FileInfo | null = null
        while (true) {
            await delay(1000)

            let ok = true
            for (let client of this.nodes) {
                if (useTxSeq) {
                    info = await client.getFileInfoByTxSeq(txSeq)
                } else {
                    info = await client.getFileInfo(root, true)
                }

                if (info === null) {
                    let logMsg = 'Waiting for storage node to sync...'

                    let status = await client.getStatus()
                    if (status !== null) {
                        const logSyncHeight = status.logSyncHeight
                        logMsg = `Waiting for storage node to sync (height=${logSyncHeight})...`
                    }

                    console.log(logMsg)
                    onProgress?.(logMsg)
                    ok = false
                    break
                }

                if (finalityRequired && !info.finalized) {
                    const msg = 'Waiting for finality confirmation...'
                    console.log(
                        'Log entry is available, but not finalized yet, ',
                        client,
                        info
                    )
                    onProgress?.(msg)
                    ok = false
                    break
                }
            }

            if (ok) {
                break
            }
        }

        return info
    }

    // Function to process all tasks in parallel
    async processTasksInParallel(
        file: AbstractFile,
        tree: MerkleTree,
        tasks: UploadTask[],
        retryOpts?: RetryOpts
    ): Promise<(number | Error)[]> {
        const taskPromises = tasks.map((task) =>
            this.uploadTask(file, tree, task, retryOpts)
        )
        return await Promise.all(taskPromises)
    }

    nextSgmentIndex(config: ShardConfig, startIndex: number): number {
        if (config.numShard < 2) {
            return startIndex
        }
        return (
            Math.floor(
                (startIndex + config.numShard - 1 - config.shardId) /
                    config.numShard
            ) *
                config.numShard +
            config.shardId
        )
    }

    async splitTasks(
        info: FileInfo,
        tree: MerkleTree,
        opts: UploadOption = {}
    ): Promise<UploadTask[] | null> {
        const mergedOpts = mergeUploadOptions(opts)
        const shardConfigs = await getShardConfigs(this.nodes)
        if (shardConfigs === null) {
            console.log('Failed to get shard configs')
            return null
        }

        if (!checkReplica(shardConfigs, mergedOpts.expectedReplica)) {
            console.log('Not enough replicas')
            return null
        }

        let txSeq = info.tx.seq

        let [startSegmentIndex, endSegmentIndex] = SegmentRange(
            info.tx.startEntryIndex,
            info.tx.size
        )
        var uploadTasks: UploadTask[][] = []

        for (
            let clientIndex = 0;
            clientIndex < shardConfigs.length;
            clientIndex++
        ) {
            const shardConfig = shardConfigs[clientIndex]
            let cInfo = await this.nodes[clientIndex].getFileInfo(
                tree.rootHash()!,
                true
            )
            if (cInfo !== null && cInfo.finalized) {
                continue
            }
            var tasks: UploadTask[] = []
            let segIndex = this.nextSgmentIndex(shardConfig, startSegmentIndex)

            while (segIndex <= endSegmentIndex) {
                tasks.push({
                    clientIndex,
                    taskSize: mergedOpts.taskSize,
                    segIndex: segIndex - startSegmentIndex,
                    numShard: shardConfig.numShard,
                    txSeq,
                })
                segIndex += shardConfig.numShard * mergedOpts.taskSize
            }

            if (tasks.length > 0) {
                uploadTasks.push(tasks)
            }
        }

        if (uploadTasks.length === 0) {
            return []
        }
        console.log('Tasks created:', uploadTasks)

        var tasks: UploadTask[] = []
        if (uploadTasks.length > 0) {
            // Longest first: both loops below stop at the first list that runs
            // out, so a shorter list in front would cut off the others' tails.
            uploadTasks.sort((a, b) => b.length - a.length)
            for (
                let taskIndex = 0;
                taskIndex < uploadTasks[0].length;
                taskIndex += 1
            ) {
                for (
                    let i = 0;
                    i < uploadTasks.length && taskIndex < uploadTasks[i].length;
                    i += 1
                ) {
                    tasks.push(uploadTasks[i][taskIndex])
                }
            }
        }
        return tasks
    }

    async getSegment(
        file: AbstractFile,
        tree: MerkleTree,
        segIndex: number
    ): Promise<[boolean, SegmentWithProof | null, Error | null]> {
        let numChunks = file.numChunks()
        let startSegIndex = segIndex * DEFAULT_SEGMENT_MAX_CHUNKS
        if (startSegIndex >= numChunks) {
            return [true, null, null]
        }

        const iter = file.iterateWithOffsetAndBatch(
            segIndex * DEFAULT_SEGMENT_SIZE,
            DEFAULT_SEGMENT_SIZE,
            true
        )

        let [ok, err] = await iter.next()
        if (!ok) {
            return [false, null, err]
        }

        let segment = iter.current()

        const proof = tree.proofAt(segIndex)

        const startIndex = segIndex * DEFAULT_SEGMENT_MAX_CHUNKS

        let allDataUploaded = false

        if (startIndex + segment.length / DEFAULT_CHUNK_SIZE >= numChunks) {
            const expectedLen = DEFAULT_CHUNK_SIZE * (numChunks - startIndex)
            segment = segment.slice(0, expectedLen)
            allDataUploaded = true
        }

        const segWithProof: SegmentWithProof = {
            root: tree.rootHash() as string,
            data: encodeBase64(segment),
            index: segIndex,
            proof: proof,
            fileSize: file.size(),
        }

        return [allDataUploaded, segWithProof, null]
    }

    async uploadTask(
        file: AbstractFile,
        tree: MerkleTree,
        uploadTask: UploadTask,
        retryOpts?: RetryOpts
    ): Promise<number | Error> {
        let segIndex = uploadTask.segIndex
        var segments: SegmentWithProof[] = []
        for (let i = 0; i < uploadTask.taskSize; i += 1) {
            let [allDataUploaded, segWithProof, err] = await this.getSegment(
                file,
                tree,
                segIndex
            )
            if (err !== null) {
                return err
            }

            if (segWithProof !== null) {
                segments.push(segWithProof)
            }

            if (allDataUploaded) {
                break
            }

            segIndex += uploadTask.numShard
        }

        // Retry logic for "too many data writing" errors
        const maxRetries = retryOpts?.TooManyDataRetries ?? 3 // Default to 3 retries
        const waitTime = (retryOpts?.Interval ?? 1) * 1000 // Default to 1 second
        let attempt = 0
        let lastError: Error | null = null

        while (attempt < maxRetries) {
            try {
                let res = await this.nodes[
                    uploadTask.clientIndex
                ].uploadSegmentsByTxSeq(segments, uploadTask.txSeq)

                return res
            } catch (error) {
                lastError = error as Error
                const nodeUrl = this.nodes[uploadTask.clientIndex].url

                // Handle "already uploaded and finalized" as success
                if (this.isAlreadyUploadedError(error)) {
                    console.log(
                        `Segments already uploaded and finalized on node ${nodeUrl}`
                    )
                    return 0 // Return success
                }

                // Handle retryable errors
                if (this.isRetryableError(error)) {
                    if (attempt < maxRetries - 1) {
                        const errorType = this.getErrorType(error)

                        console.log(
                            `${errorType} on attempt ${
                                attempt + 1
                            }/${maxRetries}. Retrying in ${waitTime / 1000}s...`
                        )

                        await new Promise((resolve) =>
                            setTimeout(resolve, waitTime)
                        )
                        attempt++
                        continue
                    } else {
                        const errorMessage =
                            error instanceof Error
                                ? error.message
                                : String(error)
                        console.log(
                            `Max retries (${maxRetries}) reached for error: ${errorMessage}`
                        )
                        return new Error(
                            `Failed after ${maxRetries} attempts: ${errorMessage}`
                        )
                    }
                } else {
                    // Non-retryable error - fail immediately
                    const errorMessage =
                        error instanceof Error ? error.message : String(error)
                    console.log(
                        `Non-retryable error encountered: ${errorMessage}`
                    )
                    return lastError
                }
            }
        }

        const finalError =
            lastError ||
            new Error(
                `Upload failed after ${maxRetries} attempts to node ${
                    this.nodes[uploadTask.clientIndex].url
                }`
            )
        console.log(`Upload task failed completely:`, finalError.message)
        return finalError
    }

    private isAlreadyUploadedError(error: any): boolean {
        return (
            error instanceof Error &&
            ((error as any).data?.includes?.(
                'already uploaded and finalized'
            ) ||
                (error.message.includes('Invalid params') &&
                    (error as any).data === 'already uploaded and finalized'))
        )
    }

    private isRetryableError(error: any): boolean {
        return (
            error instanceof Error &&
            (error.message.includes('too many data writing') ||
                error.message.includes('returned null for upload segments') ||
                (error as any).data?.message?.includes('too many data writing'))
        )
    }

    private getErrorType(error: any): string {
        if (error.message.includes('too many data writing')) {
            return '"too many data writing" error'
        } else if (error.message.includes('returned null')) {
            return 'null response error'
        } else {
            return 'retryable error'
        }
    }
}
