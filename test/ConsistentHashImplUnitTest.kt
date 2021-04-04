import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import kotlin.random.Random

class ConsistentHashImplUnitTest {
    @Test
    fun testAddSingleShard() {
        val random = Random(System.currentTimeMillis())
        val cHash = ConsistentHashImpl<Int>()
        val shard1 = Shard(shardName = "shard_1")
        val addRes = cHash.addShard(newShard = shard1, vnodeHashes = setOf(100))
        assertEquals(emptyMap<Shard, Set<HashRange>>(), addRes)
        repeat(100) {
            val key = random.nextInt()
            assertEquals(shard1, cHash.getShardByKey(key))
        }
    }

    @Test
    fun testAddMultipleVnodesSingleShard() {
        val random = Random(System.currentTimeMillis())
        val cHash = ConsistentHashImpl<Int>()
        val shard1 = Shard(shardName = "shard_1")
        val addRes = cHash.addShard(
            newShard = shard1,
            vnodeHashes = setOf(-100_000, 100_000, 300_000, 500_000, 900_000)
        )
        assertEquals(emptyMap<Shard, Set<HashRange>>(), addRes)
        repeat(100) {
            val key = random.nextInt()
            assertEquals(shard1, cHash.getShardByKey(key))
        }
    }

    @Test
    fun testAddMultipleShardsSingleVnode() {
        val cHash = ConsistentHashImpl<Int>()

        val shard1 = Shard(shardName = "shard_1")
        var addRes = cHash.addShard(newShard = shard1, vnodeHashes = setOf(100))
        assertEquals(emptyMap<Shard, Set<HashRange>>(), addRes)

        val shard2 = Shard(shardName = "shard_2")
        addRes = cHash.addShard(newShard = shard2, vnodeHashes = setOf(200))
        assertEquals(
            mapOf(
                Pair(shard1, setOf(HashRange(leftBorder = 101, rightBorder = 200)))
            ),
            addRes
        )

        val shard3 = Shard(shardName = "shard_3")
        addRes = cHash.addShard(newShard = shard3, vnodeHashes = setOf(150))
        assertEquals(
            mapOf(
                Pair(shard2, setOf(HashRange(leftBorder = 101, rightBorder = 150)))
            ),
            addRes
        )

        val shard4 = Shard(shardName = "shard_4")
        addRes = cHash.addShard(newShard = shard4, vnodeHashes = setOf(50))
        assertEquals(
            mapOf(
                Pair(shard1, setOf(HashRange(leftBorder = 201, rightBorder = 50)))
            ),
            addRes
        )

        val shard5 = Shard(shardName = "shard_5")
        addRes = cHash.addShard(newShard = shard5, vnodeHashes = setOf(500))
        assertEquals(
            mapOf(
                Pair(shard4, setOf(HashRange(leftBorder = 201, rightBorder = 500)))
            ),
            addRes
        )

        assertEquals(shard4, cHash.getShardByKey(-100))
        assertEquals(shard4, cHash.getShardByKey(10))
        assertEquals(shard1, cHash.getShardByKey(75))
        assertEquals(shard1, cHash.getShardByKey(100))
        assertEquals(shard5, cHash.getShardByKey(300))
        assertEquals(shard4, cHash.getShardByKey(1_000_000))
    }

    @Test
    fun testOneAfterAnother() {
        val cHash = ConsistentHashImpl<Int>()

        val shard1 = Shard(shardName = "shard_1")
        var addRes = cHash.addShard(newShard = shard1, vnodeHashes = setOf(100, 1000, 2000))
        assertEquals(emptyMap<Shard, Set<HashRange>>(), addRes)

        val shard2 = Shard(shardName = "shard_2")
        addRes = cHash.addShard(newShard = shard2, vnodeHashes = setOf(200, 500, 400))
        assertEquals(
            mapOf(
                Pair(shard1, setOf(HashRange(leftBorder = 101, rightBorder = 500)))
            ),
            addRes
        )

        assertEquals(shard1, cHash.getShardByKey(100))
        assertEquals(shard1, cHash.getShardByKey(-100))
        assertEquals(shard2, cHash.getShardByKey(150))
        assertEquals(shard2, cHash.getShardByKey(500))
        assertEquals(shard1, cHash.getShardByKey(501))
        assertEquals(shard1, cHash.getShardByKey(3000))
    }

    @Test
    fun testOneAfterAnotherCircleEnd() {
        val cHash = ConsistentHashImpl<Int>()

        val shard1 = Shard(shardName = "shard_1")
        var addRes = cHash.addShard(newShard = shard1, vnodeHashes = setOf(100, 1000, 2000))
        assertEquals(emptyMap<Shard, Set<HashRange>>(), addRes)

        val shard2 = Shard(shardName = "shard_2")
        addRes = cHash.addShard(newShard = shard2, vnodeHashes = setOf(3000, -100, -500, 2500))
        assertEquals(
            mapOf(
                Pair(shard1, setOf(HashRange(leftBorder = 2001, rightBorder = -100)))
            ),
            addRes
        )
    }

    @Test
    fun testMultipleRangesReplaceSameShard() {
        val cHash = ConsistentHashImpl<Int>()

        val shard1 = Shard(shardName = "shard_1")
        var addRes = cHash.addShard(newShard = shard1, vnodeHashes = setOf(300, 1200, 2200))
        assertEquals(emptyMap<Shard, Set<HashRange>>(), addRes)

        val shard2 = Shard(shardName = "shard_2")
        addRes = cHash.addShard(newShard = shard2, vnodeHashes = setOf(1700, 4200, 3200))
        assertEquals(
            mapOf(
                Pair(
                    shard1,
                    setOf(
                        HashRange(leftBorder = 1201, rightBorder = 1700),
                        HashRange(leftBorder = 2201, rightBorder = 4200)
                    )
                )
            ),
            addRes
        )

        assertEquals(shard1, cHash.getShardByKey(4201))
    }

    @Test
    fun testMultipleRangesReplaceMultipleShard() {
        val cHash = ConsistentHashImpl<Int>()

        val shard1 = Shard(shardName = "shard_1")
        var addRes = cHash.addShard(newShard = shard1, vnodeHashes = setOf(100, 1000, 2000))
        assertEquals(emptyMap<Shard, Set<HashRange>>(), addRes)

        val shard2 = Shard(shardName = "shard_2")
        addRes = cHash.addShard(newShard = shard2, vnodeHashes = setOf(1500, 4000, 3000))
        assertEquals(
            mapOf(
                Pair(
                    shard1,
                    setOf(
                        HashRange(leftBorder = 2001, rightBorder = 4000),
                        HashRange(leftBorder = 1001, rightBorder = 1500)
                    )
                )
            ),
            addRes
        )

        val shard3 = Shard(shardName = "shard_3")
        addRes = cHash.addShard(newShard = shard3, vnodeHashes = setOf(5000, -100, -200, 1300, 1250))
        assertEquals(
            mapOf(
                Pair(shard2, setOf(HashRange(leftBorder = 1001, rightBorder = 1300))),
                Pair(shard1, setOf(HashRange(leftBorder = 4001, rightBorder = -100)))
            ),
            addRes
        )
    }

    @Test
    fun testAddMultipleShardsMultipleVnodesStress() {
        val cHash = ConsistentHashImpl<Int>()

        val shard1 = Shard(shardName = "shard_1")
        var addRes = cHash.addShard(newShard = shard1, vnodeHashes = setOf(100, 1000, 2000))
        assertEquals(emptyMap<Shard, Set<HashRange>>(), addRes)

        val shard2 = Shard(shardName = "shard_2")
        addRes = cHash.addShard(newShard = shard2, vnodeHashes = setOf(200, 3000, -100))
        assertEquals(
            mapOf(
                Pair(
                    shard1,
                    setOf(
                        HashRange(leftBorder = 2001, rightBorder = -100),
                        HashRange(leftBorder = 101, rightBorder = 200)
                    )
                )
            ),
            addRes
        )

        val shard3 = Shard(shardName = "shard_3")
        addRes = cHash.addShard(newShard = shard3, vnodeHashes = setOf(300, -200, 400))
        assertEquals(
            mapOf(
                Pair(shard1, setOf(HashRange(leftBorder = 201, rightBorder = 400))),
                Pair(shard2, setOf(HashRange(leftBorder = 3001, rightBorder = -200)))
            ),
            addRes
        )

        val shard4 = Shard(shardName = "shard_4")
        addRes = cHash.addShard(newShard = shard4, vnodeHashes = setOf(1500, 1800, 1700))
        assertEquals(
            mapOf(
                Pair(shard1, setOf(HashRange(leftBorder = 1001, rightBorder = 1800)))
            ),
            addRes
        )

        val shard5 = Shard(shardName = "shard_5")
        addRes = cHash.addShard(newShard = shard5, vnodeHashes = setOf(1600, 1750, 150))
        assertEquals(
            mapOf(
                Pair(shard2, setOf(HashRange(leftBorder = 101, rightBorder = 150))),
                Pair(
                    shard4,
                    setOf(
                        HashRange(leftBorder = 1701, rightBorder = 1750),
                        HashRange(leftBorder = 1501, rightBorder = 1600)
                    )
                )
            ),
            addRes
        )

        val shard6 = Shard(shardName = "shard_6")
        addRes = cHash.addShard(newShard = shard6, vnodeHashes = setOf(4000, -300))
        assertEquals(
            mapOf(
                Pair(shard3, setOf(HashRange(leftBorder = 3001, rightBorder = -300))),
            ),
            addRes
        )

        assertEquals(shard2, cHash.getShardByKey(-100))
        assertEquals(shard4, cHash.getShardByKey(1602))
        assertEquals(shard2, cHash.getShardByKey(-150))
        assertEquals(shard3, cHash.getShardByKey(350))
    }

    @Test
    fun testAddAndRemoveSingleShard() {
        val cHash = ConsistentHashImpl<Int>()

        val shard1 = Shard(shardName = "shard_1")
        var addRes = cHash.addShard(newShard = shard1, vnodeHashes = setOf(100, 1000, 2000))
        assertEquals(emptyMap<Shard, Set<HashRange>>(), addRes)

        val shard2 = Shard(shardName = "shard_2")
        addRes = cHash.addShard(newShard = shard2, vnodeHashes = setOf(1500))
        assertEquals(
            mapOf(
                Pair(shard1, setOf(HashRange(leftBorder = 1001, rightBorder = 1500)))
            ),
            addRes
        )

        val removeRes = cHash.removeShard(shard = shard2)
        assertEquals(
            mapOf(
                Pair(shard1, setOf(HashRange(leftBorder = 1001, rightBorder = 1500)))
            ),
            removeRes
        )

        val random = Random(System.currentTimeMillis())
        repeat(100) {
            val key = random.nextInt()
            assertEquals(shard1, cHash.getShardByKey(key))
        }
    }

    @Test
    fun testAddNewAndRemoveOldShard() {
        val cHash = ConsistentHashImpl<Int>()

        val shard1 = Shard(shardName = "shard_1")
        var addRes = cHash.addShard(newShard = shard1, vnodeHashes = setOf(200, 1100, 2100))
        assertEquals(emptyMap<Shard, Set<HashRange>>(), addRes)

        val shard2 = Shard(shardName = "shard_2")
        addRes = cHash.addShard(newShard = shard2, vnodeHashes = setOf(1600))
        assertEquals(
            mapOf(
                Pair(shard1, setOf(HashRange(leftBorder = 1101, rightBorder = 1600)))
            ),
            addRes
        )

        val removeRes = cHash.removeShard(shard = shard1)
        assertEquals(
            mapOf(
                Pair(shard2, setOf(HashRange(leftBorder = 1601, rightBorder = 1100)))
            ),
            removeRes
        )

        val random = Random(System.currentTimeMillis())
        repeat(100) {
            val key = random.nextInt()
            assertEquals(shard2, cHash.getShardByKey(key))
        }
    }

    @Test
    fun testRemoveMultipleRangesSameShard() {
        val cHash = ConsistentHashImpl<Int>()

        val shard1 = Shard(shardName = "shard_1")
        var addRes = cHash.addShard(newShard = shard1, vnodeHashes = setOf(100, 500, 1000))
        assertEquals(emptyMap<Shard, Set<HashRange>>(), addRes)

        val shard2 = Shard(shardName = "shard_2")
        addRes = cHash.addShard(newShard = shard2, vnodeHashes = setOf(2000, 300))
        assertEquals(
            mapOf(
                Pair(
                    shard1,
                    setOf(
                        HashRange(leftBorder = 1001, rightBorder = 2000),
                        HashRange(leftBorder = 101, rightBorder = 300)
                    )
                )
            ),
            addRes
        )

        val shard3 = Shard(shardName = "shard_3")
        addRes = cHash.addShard(newShard = shard3, vnodeHashes = setOf(250, 200, 1500, 1700))
        assertEquals(
            mapOf(
                Pair(
                    shard2,
                    setOf(
                        HashRange(leftBorder = 1001, rightBorder = 1700),
                        HashRange(leftBorder = 101, rightBorder = 250)
                    )
                )
            ),
            addRes
        )

        val removeRes = cHash.removeShard(shard = shard3)
        assertEquals(
            mapOf(
                Pair(
                    shard2,
                    setOf(
                        HashRange(leftBorder = 1001, rightBorder = 1700),
                        HashRange(leftBorder = 101, rightBorder = 250)
                    )
                )
            ),
            removeRes
        )
    }

    @Test
    fun testRemoveMultipleRangesSameShardRangeBeginEnd() {
        val cHash = ConsistentHashImpl<Int>()

        val shard1 = Shard(shardName = "shard_1")
        var addRes = cHash.addShard(newShard = shard1, vnodeHashes = setOf(200, 1000, 2000))
        assertEquals(emptyMap<Shard, Set<HashRange>>(), addRes)

        val shard2 = Shard(shardName = "shard_2")
        addRes = cHash.addShard(newShard = shard2, vnodeHashes = setOf(3000, 1500, 500))
        assertEquals(
            mapOf(
                Pair(
                    shard1,
                    setOf(
                        HashRange(leftBorder = 201, rightBorder = 500),
                        HashRange(leftBorder = 1001, rightBorder = 1500),
                        HashRange(leftBorder = 2001, rightBorder = 3000),
                    )
                )
            ),
            addRes
        )

        val shard3 = Shard(shardName = "shard_3")
        addRes = cHash.addShard(newShard = shard3, vnodeHashes = setOf(4000, -100, 700))
        assertEquals(
            mapOf(
                Pair(
                    shard1,
                    setOf(
                        HashRange(leftBorder = 501, rightBorder = 700),
                        HashRange(leftBorder = 3001, rightBorder = -100)
                    )
                )
            ),
            addRes
        )

        val removeRes = cHash.removeShard(shard = shard3)
        assertEquals(
            mapOf(
                Pair(
                    shard1,
                    setOf(
                        HashRange(leftBorder = 501, rightBorder = 700),
                        HashRange(leftBorder = 3001, rightBorder = -100)
                    )
                )
            ),
            removeRes
        )
    }

    @Test
    fun testRemoveMultipleRangesMultipleShards() {
        val cHash = ConsistentHashImpl<Int>()

        val shard1 = Shard(shardName = "shard_1")
        var addRes = cHash.addShard(newShard = shard1, vnodeHashes = setOf(200, 1000, 2000))
        assertEquals(emptyMap<Shard, Set<HashRange>>(), addRes)

        val shard2 = Shard(shardName = "shard_2")
        addRes = cHash.addShard(newShard = shard2, vnodeHashes = setOf(3000, 4000))
        assertEquals(
            mapOf(
                Pair(
                    shard1,
                    setOf(
                        HashRange(leftBorder = 2001, rightBorder = 4000)
                    )
                )
            ),
            addRes
        )

        val shard3 = Shard(shardName = "shard_3")
        addRes = cHash.addShard(newShard = shard3, vnodeHashes = setOf(5000, -100, 700, 500, 2500))
        assertEquals(
            mapOf(
                Pair(
                    shard1,
                    setOf(
                        HashRange(leftBorder = 4001, rightBorder = -100),
                        HashRange(leftBorder = 201, rightBorder = 700)
                    )
                ),
                Pair(shard2, setOf(HashRange(leftBorder = 2001, rightBorder = 2500)))
            ),
            addRes
        )

        val removeRes = cHash.removeShard(shard = shard3)
        assertEquals(
            mapOf(
                Pair(
                    shard1,
                    setOf(
                        HashRange(leftBorder = 4001, rightBorder = -100),
                        HashRange(leftBorder = 201, rightBorder = 700)
                    )
                ),
                Pair(shard2, setOf(HashRange(leftBorder = 2001, rightBorder = 2500)))
            ),
            removeRes
        )
    }

    @Test
    fun testRemoveOldShardMultipleRangesMultipleShards() {
        val cHash = ConsistentHashImpl<Int>()

        val shard1 = Shard(shardName = "shard_1")
        var addRes = cHash.addShard(newShard = shard1, vnodeHashes = setOf(200, 1000, 2000))
        assertEquals(emptyMap<Shard, Set<HashRange>>(), addRes)

        val shard2 = Shard(shardName = "shard_2")
        addRes = cHash.addShard(newShard = shard2, vnodeHashes = setOf(3000, 4000))
        assertEquals(
            mapOf(
                Pair(
                    shard1,
                    setOf(
                        HashRange(leftBorder = 2001, rightBorder = 4000)
                    )
                )
            ),
            addRes
        )

        val shard3 = Shard(shardName = "shard_3")
        addRes = cHash.addShard(newShard = shard3, vnodeHashes = setOf(5000, -100, 700, 500, 2500))
        assertEquals(
            mapOf(
                Pair(
                    shard1,
                    setOf(
                        HashRange(leftBorder = 4001, rightBorder = -100),
                        HashRange(leftBorder = 201, rightBorder = 700)
                    )
                ),
                Pair(shard2, setOf(HashRange(leftBorder = 2001, rightBorder = 2500)))
            ),
            addRes
        )

        val removeRes = cHash.removeShard(shard = shard2)
        assertEquals(
            mapOf(
                Pair(shard3, setOf(HashRange(leftBorder = 2501, rightBorder = 4000)))
            ),
            removeRes
        )
    }

    @Test
    fun testRemoveOldestShardMultipleRangesMultipleShards() {
        val cHash = ConsistentHashImpl<Int>()

        val shard1 = Shard(shardName = "shard_1")
        var addRes = cHash.addShard(newShard = shard1, vnodeHashes = setOf(200, 1000, 2000))
        assertEquals(emptyMap<Shard, Set<HashRange>>(), addRes)

        val shard2 = Shard(shardName = "shard_2")
        addRes = cHash.addShard(newShard = shard2, vnodeHashes = setOf(3000, 4000))
        assertEquals(
            mapOf(
                Pair(
                    shard1,
                    setOf(
                        HashRange(leftBorder = 2001, rightBorder = 4000)
                    )
                )
            ),
            addRes
        )

        val shard3 = Shard(shardName = "shard_3")
        addRes = cHash.addShard(newShard = shard3, vnodeHashes = setOf(5000, -100, 700, 500, 2500))
        assertEquals(
            mapOf(
                Pair(
                    shard1,
                    setOf(
                        HashRange(leftBorder = 4001, rightBorder = -100),
                        HashRange(leftBorder = 201, rightBorder = 700)
                    )
                ),
                Pair(shard2, setOf(HashRange(leftBorder = 2001, rightBorder = 2500)))
            ),
            addRes
        )

        val removeRes = cHash.removeShard(shard = shard1)
        assertEquals(
            mapOf(
                Pair(
                    shard3,
                    setOf(
                        HashRange(leftBorder = 701, rightBorder = 2000),
                        HashRange(leftBorder = -99, rightBorder = 200)
                    )
                )
            ),
            removeRes
        )
    }

    @Test
    fun testExpandingEndBeginPrevious() {
        val cHash = ConsistentHashImpl<Int>()

        val shard1 = Shard(shardName = "shard_1")
        var addRes = cHash.addShard(newShard = shard1, vnodeHashes = setOf(1000))
        assertEquals(emptyMap<Shard, Set<HashRange>>(), addRes)

        val shard2 = Shard(shardName = "shard_2")
        addRes = cHash.addShard(newShard = shard2, vnodeHashes = setOf(2000, 100))
        assertEquals(
            mapOf(
                Pair(
                    shard1,
                    setOf(
                        HashRange(leftBorder = 1001, rightBorder = 100)
                    )
                )
            ),
            addRes
        )
    }

    @Test
    fun testExpandingBeginEndPrevious() {
        val cHash = ConsistentHashImpl<Int>()

        val shard1 = Shard(shardName = "shard_1")
        var addRes = cHash.addShard(newShard = shard1, vnodeHashes = setOf(1000))
        assertEquals(emptyMap<Shard, Set<HashRange>>(), addRes)

        val shard2 = Shard(shardName = "shard_2")
        addRes = cHash.addShard(newShard = shard2, vnodeHashes = setOf(2000, 3000))
        assertEquals(
            mapOf(
                Pair(
                    shard1,
                    setOf(
                        HashRange(leftBorder = 1001, rightBorder = 3000)
                    )
                )
            ),
            addRes
        )
    }

    @Test
    fun testExpandingPreviousEndBegin() {
        val cHash = ConsistentHashImpl<Int>()

        val shard1 = Shard(shardName = "shard_1")
        var addRes = cHash.addShard(newShard = shard1, vnodeHashes = setOf(1000))
        assertEquals(emptyMap<Shard, Set<HashRange>>(), addRes)

        val shard2 = Shard(shardName = "shard_2")
        addRes = cHash.addShard(newShard = shard2, vnodeHashes = setOf(100, 200))
        assertEquals(
            mapOf(
                Pair(
                    shard1,
                    setOf(
                        HashRange(leftBorder = 1001, rightBorder = 200)
                    )
                )
            ),
            addRes
        )
    }
}