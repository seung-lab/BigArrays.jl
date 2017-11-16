module Chunks

using EMIRT
using HDF5

abstract type AbstractChunk end

export Chunk, blendchunk, crop_border, physical_offset
export save, savechunk, readchunk, downsample, get_offset, get_offset, get_voxel_size, get_data, get_start, get_start

struct Chunk <: AbstractChunk
    data::Union{Array, SegMST} # could be 3 or 4 Dimensional array
    start::Vector{Int}     # measured by voxel number
    voxelSize::Vector{UInt32}  # physical size of each voxel
end

function Base.eltype( chk::Chunk )
    eltype(chk.data)
end

function get_data(chk::Chunk)
    chk.data
end

function get_start(chk::Chunk)
    chk.start
end
get_origin = get_start 

function get_offset(chk::Chunk)
    chk.start.-1
end

function get_voxel_size(chk::Chunk)
    chk.voxelSize
end

"""
blend chunk to BigArray
"""
function blendchunk(ba::AbstractArray, chunk::Chunk)
    gr = map((x,y)->x:x+y-1, chunk.start, size(chunk))
    @show gr
    @show size(chunk.data)
    ba[gr...] = chunk.data
end

function Base.size( chunk::Chunk )
    return size(chunk.data)  
end

function Base.ndims( chunk::Chunk )
    return ndims(chunk.data)
end

"""
crop the 3D surrounding margin
"""
function crop_border(chk::Chunk, cropMarginSize::Union{Vector,Tuple})
    @assert typeof(chk.data) <: Array
    @assert length(cropMarginSize) == ndims(chk.data)
    idx = map((x,y)->x+1:y-x, cropMarginSize, size(chk.data))
    data = chk.data[idx...]
    start = chk.start .+ cropMarginSize
    Chunk(data, start, chk.voxelSize)
end

"""
compute the physical offset
"""
function physical_offset( chk::Chunk )
    Vector{Int}((chk.start.-1) .* chk.voxelSize)
end

"""
save chunk in a hdf5 file
"""
function save(fname::AbstractString, chk::Chunk)
    if isfile(fname)
        println("removing existing file: $(fname)")
        rm(fname)
    end
    EMIRT.save(fname, chk.data)
    f = h5open(fname, "r+")
    f["type"] = "chunk"
    f["start"] = Vector{Int}(chk.start)
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
    start = read(f["start"])
    voxelSize = read(f["voxelSize"])
    close(f)
    return Chunk(data, start, voxelSize)
end

"""
cutout a chunk from BigArray
"""
function cutout(ba::AbstractArray, indexes::Union{UnitRange, Integer, Colon} ...)
    error("unimplemented")
end

function downsample(chk::Chunk; scale::Union{Vector, Tuple} = (2,2,1))
    return Chunk( EMIRT.downsample(chk.data; scale = scale),
                    (chk.start.-1).*[scale...].+1,
                    chk.voxelSize .* [scale[1:3]...]  )
end

end # end of module
