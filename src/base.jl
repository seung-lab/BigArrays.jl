export BigArray

include("types.jl")

"""
    BigArray
currently, assume that the array dimension (x,y,z,...) is >= 3
all the manipulation effects in the x,y,z dimension
"""
type BigArray{C} <: AbstractArray
  # context for specific backend type
  context ::C

  fname   ::AbstractString
  # block is a file
  blksz   ::Vector
  # the block file was continuously sub divided by chunks
  # chunk is a small 3D subvolume insize a block file
  chksz   ::Vector
end

function BigArray(ctx=context, fname::AbstractString="/tmp/bigarray",
                  blksz::Vector=[4096,4096,512], chksz::Vector=[256,256,32])
  @show ctx
  BigArray{typeof(ctx)}(ctx, fname, blksz, chksz)
end
