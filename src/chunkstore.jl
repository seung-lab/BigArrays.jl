module ChunkStore

using ..BigArrayContext

export AbstractChunkStore

abstract AbstractChunkStore{K,V} <: Associative{K,V}
