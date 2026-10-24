import { Uploader } from '../src.ts/transfer/Uploader'
import { FileInfo, StorageNode } from '../src.ts/node'
import { MerkleTree } from '../src.ts/file'

const SEGMENT_SIZE = 256 * 1024 // 1024 chunks of 256 bytes
const ROOT = '0x' + 'ab'.repeat(32)

function fileInfo(startEntryIndex: number, size: number): FileInfo {
    return {
        tx: {
            streamIds: [],
            data: new Uint8Array(),
            dataMerkleRoot: ROOT,
            merkleNodes: [],
            startEntryIndex,
            size,
            seq: 1,
        },
        finalized: false,
        isCached: false,
        uploadedSegNum: 0,
    }
}

// A node that has synced the log entry but holds none of the file yet.
// splitTasks only asks a node for its shard and for the file's status.
function shardNode(
    shardId: number,
    numShard: number,
    info: FileInfo
): StorageNode {
    return {
        getShardConfig: async () => ({ shardId, numShard }),
        getFileInfo: async () => info,
    } as unknown as StorageNode
}

// The file-relative segments each node is sent, grouped by node in the order
// the nodes were given. Like Uploader.uploadTask, a task covers taskSize
// segments numShard apart, stopping after the file's last segment.
async function segmentsPerNode(
    shards: number[][],
    info: FileInfo,
    taskSize: number
): Promise<number[][]> {
    const nodes = shards.map(([shardId, numShard]) =>
        shardNode(shardId, numShard, info)
    )
    const uploader = new Uploader(nodes, 'http://127.0.0.1:8545', {} as never)
    const tree = { rootHash: () => ROOT } as unknown as MerkleTree

    const tasks = await uploader.splitTasks(info, tree, { taskSize })
    if (tasks === null) {
        throw new Error('splitTasks returned null')
    }

    const numSegments = Math.ceil(info.tx.size / SEGMENT_SIZE)
    const perNode: number[][] = nodes.map(() => [])
    for (const task of tasks) {
        for (let i = 0; i < task.taskSize; i++) {
            const segIndex = task.segIndex + i * task.numShard
            if (segIndex >= numSegments) {
                break
            }
            perNode[task.clientIndex].push(segIndex)
        }
    }
    return perNode.map((segs) => segs.sort((a, b) => a - b))
}

describe('Uploader.splitTasks', () => {
    beforeEach(() => {
        // splitTasks logs the whole task list.
        jest.spyOn(console, 'log').mockImplementation(() => {})
    })

    afterEach(() => {
        jest.restoreAllMocks()
    })

    // Nodes are [shardId, numShard]. A node holds the flow segments whose
    // index % numShard equals its shardId; expectations are file-relative.
    test.each([
        {
            name: '2 shards, 5 segments',
            shards: [
                [0, 2],
                [1, 2],
            ],
            startEntryIndex: 0,
            size: 5 * SEGMENT_SIZE,
            taskSize: 1,
            want: [
                [0, 2, 4],
                [1, 3],
            ],
        },
        {
            name: '2 shards, 5 segments, shard-1 node listed first',
            shards: [
                [1, 2],
                [0, 2],
            ],
            startEntryIndex: 0,
            size: 5 * SEGMENT_SIZE,
            taskSize: 1,
            want: [
                [1, 3],
                [0, 2, 4],
            ],
        },
        // The Galileo upload from #59, at its real startEntryIndex.
        {
            name: '1 MiB + 50 B, whose last segment is a single chunk',
            shards: [
                [0, 2],
                [1, 2],
            ],
            startEntryIndex: 1978810368,
            size: 1048626,
            taskSize: 1,
            want: [
                [0, 2, 4],
                [1, 3],
            ],
        },
        {
            name: 'two nodes for shard 0, one for shard 1',
            shards: [
                [0, 2],
                [0, 2],
                [1, 2],
            ],
            startEntryIndex: 0,
            size: 5 * SEGMENT_SIZE,
            taskSize: 1,
            want: [
                [0, 2, 4],
                [0, 2, 4],
                [1, 3],
            ],
        },
        {
            name: '4 shards, 5 segments',
            shards: [
                [0, 4],
                [1, 4],
                [2, 4],
                [3, 4],
            ],
            startEntryIndex: 0,
            size: 5 * SEGMENT_SIZE,
            taskSize: 1,
            want: [[0, 4], [1], [2], [3]],
        },
        {
            name: 'taskSize 2, so shard 0 needs two tasks and shard 1 one',
            shards: [
                [0, 2],
                [1, 2],
            ],
            startEntryIndex: 0,
            size: 5 * SEGMENT_SIZE,
            taskSize: 2,
            want: [
                [0, 2, 4],
                [1, 3],
            ],
        },
        {
            name: 'an unsharded node next to a 2-shard pair',
            shards: [
                [0, 1],
                [0, 2],
                [1, 2],
            ],
            startEntryIndex: 0,
            size: 4 * SEGMENT_SIZE,
            taskSize: 1,
            want: [
                [0, 1, 2, 3],
                [0, 2],
                [1, 3],
            ],
        },
    ])(
        'sends every node every segment of its shard: $name',
        async ({ shards, startEntryIndex, size, taskSize, want }) => {
            expect(
                await segmentsPerNode(
                    shards,
                    fileInfo(startEntryIndex, size),
                    taskSize
                )
            ).toEqual(want)
        }
    )
})
