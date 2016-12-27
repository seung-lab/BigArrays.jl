# Note that DenseArray only works for memory stored Array
# http://docs.julialang.org/en/release-0.4/manual/arrays/#implementation
export AbstractBigArray

abstract AbstractBigArray <: AbstractArray

abstract AbstractBigArrayBackend    <: Any
