class ConsistentHashImpl<K> : ConsistentHash<K> {
    override fun getShardByKey(key: K): Shard {
        TODO("Not yet implemented")
    }

    override fun addShard(newShard: Shard, vnodeHashes: Set<Int>): Map<Shard, Set<HashRange>> {
        TODO("Not yet implemented")
    }

    override fun removeShard(shard: Shard): Map<Shard, Set<HashRange>> {
        TODO("Not yet implemented")
    }
}