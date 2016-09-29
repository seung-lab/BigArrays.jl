using EMIRT
using HDF5

abstract AbstractChunk

export Chunk, blendchunk, crop_border, physical_offset, save, savechunk, readchunk

type Chunk <: AbstractChunk
    data::Union{Array, SegMST} # could be 3 or 4 Dimensional array
    origin::Vector{UInt32}     # measured by voxel number
    voxelSize::Vector{UInt32}  # physical size of each voxel
end

"""
blend chunk to BigArray
"""
function blendchunk(ba::AbstractBigArray, chunk::Chunk)
  gr = global_range( chunk )
  @show gr
  if ndims(chunk) == 3 && isa(chunk.data, Array)
    ba[gr[1], gr[2], gr[3]] = chunk.data
  elseif ndims(chunk) == 3 && isa(chunk.data, SegMST)
    ba[gr[1], gr[2], gr[3]] = chunk.data.segmentation
  elseif ndims(chunk) == 4
    ba[gr[1], gr[2], gr[3], gr[4]] = chunk.data
  end
end

"""
get global index range
"""
function global_range( chunk::Chunk )
  x1 = chunk.origin[1];   x2 = x1 + size(chunk)[1] - 1
  y1 = chunk.origin[2];   y2 = y1 + size(chunk)[2] - 1
  z1 = chunk.origin[3];   z2 = z1 + size(chunk)[3] - 1
  if ndims(chunk) == 3
    return( x1:x2, y1:y2, z1:z2 )
  elseif ndims(chunk) == 4
    return( x1:x2, y1:y2, z1:z2, :)
  end
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
function crop_border{T}(chk::Chunk, cropMarginSize::Union{Vector{T},Tuple{T}})
  @assert typeof(chk.data) <: Array
  nd = ndims(chk.data)
  @assert nd >= 3
  sz = size(chk.data)
  @assert sz[1]>cropMarginSize[1]*2 &&
          sz[2]>cropMarginSize[2]*2 &&
          sz[3]>cropMarginSize[3]*2
    if nd == 3
        data = chk.data[cropMarginSize[1]+1:sz[1]-cropMarginSize[1],
                        cropMarginSize[2]+1:sz[2]-cropMarginSize[2],
                        cropMarginSize[3]+1:sz[3]-cropMarginSize[3]]
    elseif nd==4
        data = chk.data[cropMarginSize[1]+1:sz[1]-cropMarginSize[1],
                        cropMarginSize[2]+1:sz[2]-cropMarginSize[2],
                        cropMarginSize[3]+1:sz[3]-cropMarginSize[3], :]
    elseif nd==5
        data = chk.data[cropMarginSize[1]+1:sz[1]-cropMarginSize[1],
                        cropMarginSize[2]+1:sz[2]-cropMarginSize[2],
                        cropMarginSize[3]+1:sz[3]-cropMarginSize[3], :, :]
    else
        error("only support 3-5 D, current dataay dimention is $(nd)")
    end
    origin = chk.origin .+ cropMarginSize
    Chunk(data, origin, chk.voxelSize)
end

"""
compute the physical offset
"""
function physical_offset( chk::Chunk )
    Vector{UInt32}((chk.origin.-UInt32(1)) .* chk.voxelSize)
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
    if isa(chk.data, AffinityMap)
        # save with compression
        f["affinityMap", "chunk", (64,64,8,3), "shuffle", (), "deflate", 3] = chk.data
    elseif isa(chk.data, EMImage)
        f["image", "chunk", (64,64,8), "shuffle", (), "deflate", 3] = chk.data
    elseif isa(chk.data, Segmentation)
        f["segmentation", "chunk", (64,64,8), "shuffle", (), "deflate", 3] = chk.data
    elseif isa(chk.data, SegMST)
        f["segmentation", "chunk", (64,64,8), "shuffle", (), "deflate", 3] = chk.data.segmentation
        f["segmentPairs"] = chk.data.segmentPairs
        f["segmentPairAffinities"] = chk.data.segmentPairAffinities
    else
        error("This is an unsupported type: $(typeof(chk.data))")
    end
    f["origin"] = Vector{UInt32}(chk.origin)
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
function cutout(ba::AbstractBigArray, indexes::Union{UnitRange, Integer, Colon} ...)
    error("unimplemented")
end
