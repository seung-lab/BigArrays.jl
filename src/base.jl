"""
    BigArray
currently, assume that the array dimension (x,y,z,...) is >= 3
all the manipulation effects in the x,y,z dimension
"""
type BigArray{C} <: AbstractArray
    # the place to store this big array on disk
    # could be a local path or in cloud path.
    fstore::AbstractString
    # context for specific backend type
    context::C
end

function BigArray(fstore::AbstractString=tempname(), ctx=compute_context)
    BigArray{typeof(ctx)}(fstore,  ctx)
end
