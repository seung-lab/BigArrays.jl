module Chunks

using EMIRT
using HDF5

abstract AbstractChunk

export Chunk, blendchunk, global_range, crop_border, physical_offset
export save, savechunk, readchunk, downsample, get_offset, get_offset, get_voxel_size, get_data

immutable Chunk <: AbstractChunk
    data::Union{Array, SegMST} # could be 3 or 4 Dimensional array
    origin::Vector{Int}     # measured by voxel number
    voxelSize::Vector{UInt32}  # physical size of each voxel
end

function Base.eltype( chk::Chunk )
    eltype(chk.data)
end

function get_data(chk::Chunk)
    chk.data
end

function get_origin(chk::Chunk)
    chk.origin
end

function get_offset(chk::Chunk)
    chk.origin.-1
end

function get_voxel_size(chk::Chunk)
    chk.voxelSize
end

"""
blend chunk to BigArray
"""
function blendchunk(ba::AbstractArray, chunk::Chunk)
    gr = global_range( chunk )
    T = eltype(ba)
    if T == eltype(chunk.data)
        ba[gr...] = chunk.data
    else 
        ba[gr...] = Array{T}(chunk.data)
    end
end

"""
get global index range
"""
function global_range( chunk::Chunk )
    map((x,y)->x:x+y-1, chunk.origin, size(chunk))
end

function Base.size( chunk::Chunk )
  if isa(chunk.data, Array)
    return size(chunk.data)
  elseif isa(chunk.data, SegMST)
    return size(chunk.data.segmentation)
  else
    error("the chunk data type is invalid: $(typeof(chunk.data))")
  end
end

function Base.ndims( chunk::Chunk )
  if isa(chunk.data, Array)
    return ndims(chunk.data)
  elseif isa(chunk.data, SegMST)
    return ndims(chunk.data.segmentation)
  else
    error("the chunk data type is invalid: $(typeof(chunk.data))")
  end
end

"""
crop the 3D surrounding margin
"""
function crop_border(chk::Chunk, cropMarginSize::Union{Vector,Tuple})
    @assert typeof(chk.data) <: Array
    @assert length(cropMarginSize) == ndims(chk.data)
    idx = map((x,y)->x+1:y-x, cropMarginSize, size(chk.data))
    data = chk.data[idx...]
    origin = chk.origin .+ cropMarginSize
    Chunk(data, origin, chk.voxelSize)
end

"""
compute the physical offset
"""
function physical_offset( chk::Chunk )
    Vector{Int}((chk.origin.-1) .* chk.voxelSize)
end

"""
save chunk in a hdf5 file
"""
function save(fname::AbstractString, chk::Chunk)
    if isfile(fname)
        rm(fname)
    end
    f = h5open(fname, "w")
    f["type"] = "chunk"
    if isa(chk.data, Array{Float32,4})
        # save with compression
        f["affinityMap", "chunk", (64,64,8,3), "shuffle", (), "deflate", 3] = chk.data
    elseif isa(chk.data, Array{UInt8,3})
        f["image", "chunk", (64,64,8), "shuffle", (), "deflate", 3] = chk.data
    elseif isa(chk.data, Array{UInt32,3})
        f["segmentation", "chunk", (64,64,8), "shuffle", (), "deflate", 3] = chk.data
    elseif :segmentation in fieldnames(typeof(chk.data))
        f["segmentation", "chunk", (64,64,8), "shuffle", (), "deflate", 3] = chk.data.segmentation
        f["segmentPairs"] = chk.data.segmentPairs
        f["segmentPairAffinities"] = chk.data.segmentPairAffinities
    else
        error("This is an unsupported type: $(typeof(chk.data))")
    end
    f["origin"] = Vector{Int}(chk.origin)
    f["voxelSize"] = Vector{UInt32}(chk.voxelSize)
    close(f)
end
savechunk = save

function readchunk(fname::AbstractString)
    f = h5open(fname)
    if has(f, "main")
        data = read(f["main"])
    elseif has(f, "affinityMap")
        data = read(f["affinityMap"])
    elseif has(f, "image")
        data = read(f, "image")
    elseif has(f, "segmentPairs")
      data = readsgm(fname)
    elseif has(f, "segmentation")
        data = readseg(fname)
    else
        error("not a standard chunk file")
    end
    origin = read(f["origin"])
    voxelSize = read(f["voxelSize"])
    close(f)
    return Chunk(data, origin, voxelSize)
end

"""
cutout a chunk from BigArray
"""
function cutout(ba::AbstractArray, indexes::Union{UnitRange, Integer, Colon} ...)
    error("unimplemented")
end

function downsample(chk::Chunk; scale::Union{Vector, Tuple} = (2,2,1))
    return Chunk( EMIRT.downsample(chk.data; scale = scale),
                    (chk.origin.-1).*[scale...].+1,
                    chk.voxelSize .* [scale[1:3]...]  )
end

end # end of module
