module H5sBigArrays
using ..BigArrays
using ..BigArrays.Iterators
using HDF5
using JSON
using Blosc

# Blosc.set_num_threads(Threads.nthreads())

# include("../types.jl")
# include("../index.jl")
const CONFIG_FILE = "config.json"
const DEFAULT_H5FILE_PREFIX = "chunk_"
const H5_DATASET_NAME = "img"
const DEFAULT_CHUNK_SIZE = (1024, 1024, 128)
const DEFAULT_INNER_CHUNK_SIZE = (32,32,4)
const DEFAULT_GLOBAL_OFFSET = (0,0,0)
const DEFAULT_COMPRESSION = :deflate

export H5sBigArray, boundingbox

"""
definition of h5s big array
"""
type H5sBigArray <: AbstractBigArray
    h5FilePrefix  ::AbstractString
    globalOffset  ::Tuple
    chunkSize     ::Tuple
    innerChunkSize::Tuple
    compression   ::Symbol              # deflate || blosc
end

"""
default constructor
"""
function H5sBigArray()
    H5sBigArray(string(tempname(), ".h5sbigarray"))
end

"""
construct a H5sBigArray from a dict
"""
function H5sBigArray( configDict::Dict{Symbol, Any} )
    if isa(configDict[:globalOffset], Vector)
        configDict[:globalOffset] = (configDict[:globalOffset]...)
    end
    if isa(configDict[:chunkSize], Vector)
        configDict[:chunkSize] = (configDict[:chunkSize]...)
    end
    if isa(configDict[:innerChunkSize], Vector)
        configDict[:innerChunkSize] = (configDict[:innerChunkSize]...)
    end
    configDict[:compression] = Symbol(configDict[:compression])
    H5sBigArray(    configDict[:h5FilePrefix],
                    configDict[:globalOffset],
                    configDict[:chunkSize],
                    configDict[:innerChunkSize],
                    configDict[:compression] )
end
"""
construct from a register file, which defines file architecture
"""
function H5sBigArray{N}(   dir::AbstractString;
                        h5FilePrefix::AbstractString    = DEFAULT_H5FILE_PREFIX,
                        globalOffset::NTuple{N}         = DEFAULT_GLOBAL_OFFSET,
                        chunkSize::NTuple{N}            = DEFAULT_CHUNK_SIZE,
                        innerChunkSize::NTuple{N}       = DEFAULT_INNER_CHUNK_SIZE,
                        compression::Symbol             = DEFAULT_COMPRESSION)
    dir = expanduser(dir)
    configFile = joinpath(dir, CONFIG_FILE)
    if isfile(dir)
        warn("take this file as bigarray config file: $(dir)")
        global H5SBIGARRAY_DIRECTORY = dirname(dir)
        # string format of config
        configDict = JSON.parsefile(dir, dicttype=Dict{Symbol, Any})
        @show configDict
        ba = H5sBigArray( configDict )
    elseif isdir(dir) && isfile(configFile)
        global H5SBIGARRAY_DIRECTORY = dir
        # string format of config
        configDict = JSON.parsefile(configFile, dicttype=Dict{Symbol, Any})
        @show configDict
        ba = H5sBigArray( configDict )
    else
        if !isdir(dir)
          mkdir(dir)
        end
        global H5SBIGARRAY_DIRECTORY = dir
        ba = H5sBigArray(h5FilePrefix, globalOffset, chunkSize,
                            innerChunkSize, compression)
        updateconfigfile(ba)
    end
    ba
end


"""
transform bigarray to string
"""
function bigArray2dict(ba::H5sBigArray)
    d = Dict{Symbol, Any}()
    d[:h5FilePrefix]    = ba.h5FilePrefix
    d[:globalOffset]    = ba.globalOffset
    d[:chunkSize]       = ba.chunkSize
    d[:innerChunkSize]  = ba.innerChunkSize
    d[:compression]     = ba.compression
    return d
end

function bigArray2string(ba::H5sBigArray)
    d = bigArray2dict(ba)
    JSON.json(d)
end

"""
update the config.json file
"""
function updateconfigfile(ba::H5sBigArray)
  configFile = joinpath(H5SBIGARRAY_DIRECTORY, CONFIG_FILE)
  if !isdir(H5SBIGARRAY_DIRECTORY)
      mkdir(H5SBIGARRAY_DIRECTORY)
  end
  str = bigArray2string(ba)
  # write to text file
  f = open(configFile, "w")
  write(f, str)
  close(f)
end

"""
element type of big array
"""
function Base.eltype(ba::H5sBigArray)
  files = readdir(H5SBIGARRAY_DIRECTORY)
  for file in files
    h5FileName = joinpath(H5SBIGARRAY_DIRECTORY, file)
    if ishdf5(h5FileName)
      f = h5open(h5FileName)
      ret = eltype(f[H5_DATASET_NAME])
      close(f)
      return ret
    end
  end
end

"""
number of dimension
"""
function Base.ndims(ba::H5sBigArray)
  for file in readdir(H5SBIGARRAY_DIRECTORY)
    fileName = joinpath(H5SBIGARRAY_DIRECTORY, file)
    if ishdf5(fileName)
      f = h5open(fileName)
      ret = ndims(f[H5_DATASET_NAME])
      close(f)
      return ret
    end
  end
end

"""
bounding box of the whole volume
"""
function boundingbox(ba::H5sBigArray)
    D = ndims(ba)
    ret_start = CartesianIndex([typemax(Int) for i = 1:D]...)
    ret_stop  = CartesianIndex([typemin(Int) for i = 1:D]...)

    @show H5SBIGARRAY_DIRECTORY
    for file in readdir(H5SBIGARRAY_DIRECTORY)
        fileName = joinpath(H5SBIGARRAY_DIRECTORY, file)
        @show fileName
        if fileName[end-2:end]==".h5"
            chunkStart = fileName2origin(file)
            @show chunkStart
            # f = h5open(fileName)
            # sz = size(f[H5_DATASET_NAME])
            # close(f)
            chunkStop = map((x,y)->x+y-1, chunkStart, ba.chunkSize)
            ret_start = CartesianIndex(map(min, ret_start, chunkStart))
            ret_stop  = CartesianIndex(map(max, ret_stop,  chunkStop))
        end
    end
    return CartesianRange(ret_start, ret_stop)
end

"""
compute size from bounding box
"""
function Base.size(ba::H5sBigArray)
    size(boundingbox(ba))
end

function Base.size(ba::H5sBigArray, i::Int)
    size(ba)[i]
end

function Base.show(ba::H5sBigArray)
    # println("element type: $(eltype(ba))")
    # println("size: $(size(ba))")
    # println("bounding box: $(bbox(ba))")
    println("the data is in disk, not shown here.")
end

"""
get h5 file name
"""
function get_filename(ba::H5sBigArray, globalOrigin::Union{Tuple, Vector})
    h5FileName = ba.h5FilePrefix
    for i in 1:length(globalOrigin)
        h5FileName *= "$(globalOrigin[i])-$(globalOrigin[i]+ba.chunkSize[i]-1)_"
    end
    return joinpath(H5SBIGARRAY_DIRECTORY, "$(h5FileName[1:end-1]).h5")
end

"""
    h5read{N}(chunkFileName::AbstractString,
                    H5_DATASET_NAME::AbstractString,
                    rangeInChunk::CartesianRange{CartesianIndex{N}})

read h5 file using CartesianRange.
"""
function HDF5.h5read{N}(chunkFileName::AbstractString,
                    H5_DATASET_NAME::AbstractString,
                    rangeInChunk::CartesianRange{CartesianIndex{N}})
    blockIndexes = cartesian_range2unitrange( rangeInChunk )
    h5read(chunkFileName, H5_DATASET_NAME, blockIndexes)
end

"""
extract chunk from a bigarray
"""
function Base.getindex(ba::H5sBigArray, idxes::Union{UnitRange, Int, Colon}...)
    # clarify the Colon
    idxes = colon2unitRange(size(ba), idxes)
    # only support 3D image now, could support arbitrary dimensions in the future
    # allocate memory
    sz = map(length, idxes)
    buf = zeros(eltype(ba), sz)

    # transform to originate from (0,0,0)
    idxes = map((x,y)-> x-y, idxes, ba.globalOffset)
    bufferGlobalRange = CartesianRange(idxes)

    baIter = BigArrayIterator(bufferGlobalRange, ba.chunkSize)
    for (chunkID, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer) in baIter
        chunkFileName = get_block_file_name(ba, chunkID)
        info("read $(globalRange) from $(rangeInChunk) of $(chunkFileName) to buffer $(rangeInBuffer) ...")

        # if have data fill with data,
        # if not, no need to change, keep as zero
        if isfile(chunkFileName) && ishdf5(chunkFileName)
            # assign data value, preserve existing value
            while true
                try
                    buf[rangeInBuffer] = h5read(chunkFileName, H5_DATASET_NAME,
                                                rangeInChunk)
                    break
                catch
                    rethrow()
                    warn("open and read $chunkFileName failed, will try 5 seconds later...")
                    sleep(5)
                end
            end
        else
            warn("filled with zeros because file do not exist: $(chunkFileName)")
        end
    end
    buf
end

"""
get block file name from a
"""
function get_block_file_name{N}( ba::H5sBigArray, chunkID::NTuple{N})
    chunkGlobalRange = chunkid2global_range( chunkID, ba.chunkSize )

    fileName = ba.h5FilePrefix
    for i in 1:N
        fileName *= "$(chunkGlobalRange.start[i])-$(chunkGlobalRange.stop[i])_"
    end
    return joinpath(H5SBIGARRAY_DIRECTORY, "$(fileName[1:end-1]).h5")
end
function get_block_file_name(ba::H5sBigArray, idx::CartesianIndex)
    chunkID = index2blockid( idx, ba.chunkSize )
    get_block_file_name(ba, chunkID)
end

"""
put small array to big array
"""
function Base.setindex!{T,N}(ba::H5sBigArray, buf::Array{T,N},
                                idxes::Union{UnitRange, Int, Colon}...)
    @assert N == length(idxes)
    # clarify the Colon
    idxes = colon2unitRange(buf, idxes)
    # set bounding box
    # adjust_range!(ba, idxes)
    # updateconfigfile(ba)
    # transform to originate from (0,0,0)
    idxes = map((x,y)-> x-y, idxes, ba.globalOffset)
    bufferGlobalRange = CartesianRange(idxes)

    baIter = BigArrayIterator(bufferGlobalRange, ba.chunkSize)

    # temporal block as a buffer to reduce memory allocation
    for (chunkID, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer) in baIter
        # refresh the temporal block
        # map((x,y)->tempBlock[x]=buf[y], rangeInChunk, rangeInBuffer)
        chunkFileName = get_block_file_name(ba, chunkID)
        info("save $(globalRange) from buffer $(rangeInBuffer) to $(rangeInChunk) of $(chunkFileName) ...")
        while true
            try
                save_buffer( buf, chunkFileName, ba, rangeInChunk, rangeInBuffer)
                info("save $(globalRange) from buffer $(rangeInBuffer) to $(rangeInChunk) of $(chunkFileName) ...")
                break
            catch
                rethrow()
                warn("open and write $h5FileName failed, will try 5 seconds later...")
                sleep(5)
            end
        end
    end
end

"""
decode file name to origin coordinate
to-do: support negative coordinate.
"""
function fileName2origin( fileName::AbstractString )
    # fileName = replace(fileName, DEFAULT_H5FILE_PREFIX, "")
    fileName = replace(fileName, ".h5", "")
    fileName = replace(fileName, "-",  ":")
    fileName = replace(fileName, "_:", "_-")
    secs = split(fileName, "_")
    origin = zeros(Int, length(secs)-1)
    for i in 1:length(origin)
        origin[i] = parse( split(secs[i+1],":")[1] )
    end
    return origin
end

"""
save part of or whole buffer to one hdf5 file
"""
function save_buffer{T,N}(  buf::Array{T,N}, chunkFileName::AbstractString,
                            ba::AbstractBigArray,
                            rangeInChunk ::CartesianRange{CartesianIndex{N}},
                            rangeInBuffer::CartesianRange{CartesianIndex{N}})
    if isfile(chunkFileName) && ishdf5(chunkFileName)
        f = h5open(chunkFileName, "r+")
        dataSet = f[H5_DATASET_NAME]
        @assert eltype(f[H5_DATASET_NAME])==T
    else
        f = h5open(chunkFileName, "w")
        dataSet = d_create(f, H5_DATASET_NAME, datatype(eltype(buf)),
            dataspace(ba.chunkSize...),
            "chunk", ba.innerChunkSize,
            "blosc", 5)
    end
    dataSet[rangeInChunk] = buf[rangeInBuffer]
    close(f)
end


function Base.setindex!{T,N}(dataSet::HDF5.HDF5Dataset, buf::Array{T,N},
                                rangeInChunk::CartesianRange{CartesianIndex{N}})
    ur = cartesian_range2unitrange(rangeInChunk)
    @show ur
    dataSet[ur...] = buf
end

end # end of module: H5sBigArrays
