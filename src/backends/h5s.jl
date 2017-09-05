module H5sBigArrays
using ..BigArrays
using ..BigArrays.BigArrayIterators
using ..BigArrays.Utils
using ..BigArrays.Index
using HDF5
using JSON
using Blosc

# make the thread number to be the number of physical cores
Blosc.set_num_threads(div(Base.Sys.CPU_CORES,2))

# include("../types.jl")
# include("../index.jl")
const CONFIG_FILE = "config.json"
const DEFAULT_DATA_TYPE = UInt8
const DEFAULT_H5FILE_PREFIX = "block_"
const H5_DATASET_NAME = "img"
const DEFAULT_BLOCK_SIZE = (1024, 1024, 128)
const DEFAULT_CHUNK_SIZE = (32,32,4)
const DEFAULT_GLOBAL_OFFSET = (0,0,0)
const DEFAULT_RANGE         = CartesianRange(
        CartesianIndex((typemax(Int), typemax(Int), typemax(Int))),
        CartesianIndex((0,0,0)))

const DEFAULT_COMPRESSION = :blosc

export H5sBigArray, boundingbox, get_chunk_size

"""
definition of h5s big array
"""
type H5sBigArray <: AbstractBigArray
    h5FilePrefix    ::AbstractString
    dataType        ::DataType
    globalOffset    ::Tuple
    blockSize       ::Tuple
    chunkSize       ::Tuple
    compression     ::Symbol              # deflate || blosc
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
    if isa(configDict[:blockSize], Vector)
        configDict[:blockSize] = (configDict[:blockSize]...)
    end
    if isa(configDict[:chunkSize], Vector)
        configDict[:chunkSize] = (configDict[:chunkSize]...)
    end
    if isa(configDict[:dataType], AbstractString)
        configDict[:dataType] = eval(Symbol(configDict[:dataType]))
    end 
    configDict[:compression] = Symbol(configDict[:compression])
    H5sBigArray(    configDict[:h5FilePrefix],
                    configDict[:dataType],
                    configDict[:globalOffset],
                    configDict[:blockSize],
                    configDict[:chunkSize],
                    configDict[:compression] )
end
"""
construct from a register file, which defines file architecture
"""
function H5sBigArray(   dir::AbstractString; 
                        h5FilePrefix::AbstractString = DEFAULT_H5FILE_PREFIX,
                        dataType        = DEFAULT_DATA_TYPE,
                        chunkSize       = DEFAULT_CHUNK_SIZE,
                        blockSize       = DEFAULT_BLOCK_SIZE,
                        globalOffset    = DEFAULT_GLOBAL_OFFSET,
                        compression     = DEFAULT_COMPRESSION )
    # transform string to Julia DataType
    if isa(dataType, AbstractString)
        dataType = eval(Symbol(dataType))
    end 
    dir = expanduser(dir)
    configFile = joinpath(dir, CONFIG_FILE)
    if isfile(dir)
        warn("find an existing config file: $(dir) \n will ignore the input parameters!")
        global H5SBIGARRAY_DIRECTORY = dirname(dir)
        # string format of config
        configDict = JSON.parsefile(dir, dicttype=Dict{Symbol, Any})
        # @show configDict
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
        ba = H5sBigArray(h5FilePrefix, dataType, globalOffset, blockSize, chunkSize, compression) 
        updateconfigfile(ba)
    end
    ba
end

function get_block_size(ba::H5sBigArray)
    ba.blockSize
end

"""
transform bigarray to string
"""
function bigArray2dict(ba::H5sBigArray)
    d = Dict{Symbol, Any}()
    d[:h5FilePrefix]    = ba.h5FilePrefix
    d[:dataType]        = ba.dataType
    d[:globalOffset]    = ba.globalOffset
    d[:blockSize]       = ba.blockSize
    d[:chunkSize]  = ba.chunkSize
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


function Base.display(ba::H5sBigArray)
    for fieldName in fieldnames(ba)
        println("$fieldName : $(getfield(ba, fieldName))")
    end
end

"""
element type of big array
"""
function Base.eltype(ba::H5sBigArray)
    return ba.dataType 
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
    ret_start = ([div(typemax(Int),2) for i = 1:D]...)
    ret_stop  = ([div(typemin(Int),2) for i = 1:D]...)

    # @show H5SBIGARRAY_DIRECTORY
    for file in readdir(H5SBIGARRAY_DIRECTORY)
        fileName = joinpath(H5SBIGARRAY_DIRECTORY, file)
        if fileName[end-2:end]==".h5"
            blockStart = fileName2origin(file; prefix = basename(ba.h5FilePrefix))
            blockStop = map((x,y)->x+y-1, blockStart, ba.blockSize)
            ret_start = (map(min, ret_start, blockStart))
            ret_stop  = (map(max, ret_stop,  blockStop))
        end
    end
    return CartesianRange(CartesianIndex(ret_start), CartesianIndex(ret_stop))
end

"""
compute size from bounding box
"""
function Base.size(ba::H5sBigArray)
    sz = size(boundingbox(ba))
    # @show boundingbox(ba)
    # @show sz
    if any(x->x<=0, sz)
        return ([0 for i = 1:ndims(ba)]...)
    else
        return sz
    end
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
        h5FileName *= "$(globalOrigin[i])-$(globalOrigin[i]+ba.blockSize[i]-1)_"
    end
    return joinpath(H5SBIGARRAY_DIRECTORY, "$(h5FileName[1:end-1]).h5")
end

"""
    Base.keys(ba::H5sBigArray)

"""
function Base.keys(ba::H5sBigArray)
    fileNames = readdir(dirname(H5SBIGARRAY_DIRECTORY))
    for i in eachindex(fileNames)
        if fileNames[i] == CONFIG_FILE
            splice!(fileNames, i)
            break
        end
    end
    return fileNames
end

function Base.getindex(ba::H5sBigArray, key::String)
    return h5read(joinpath(H5SBIGARRAY_DIRECTORY, key), H5_DATASET_NAME)
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
    blockIndexes = cartesianrange2unitrange( rangeInChunk )
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

    baIter = BigArrayIterator(bufferGlobalRange, ba.blockSize)
    @sync begin
        for (chunkID, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer) in baIter
            @async begin 
                # local chunkFileName
                chunkFileName = get_block_file_name(ba, chunkID)
                info("read $(globalRange) from $(rangeInChunk) of $(chunkFileName) to buffer $(rangeInBuffer) ...")

                # if have data fill with data,
                # if not, no need to change, keep as zero
                if isfile(chunkFileName) && ishdf5(chunkFileName)
                    buf[cartesianrange2unitrange(rangeInBuffer)...] = 
                        h5read(chunkFileName, H5_DATASET_NAME,
                               cartesianrange2unitrange(rangeInChunk))
                else
                    warn("filled with zeros because file do not exist: $(chunkFileName)")
                end
            end 
        end
    end 
    buf
end

"""
get block file name from a
"""
function get_block_file_name{N}( ba::H5sBigArray, chunkID::NTuple{N})
    chunkGlobalRange = chunkid2global_range( chunkID, ba.blockSize )

    fileName = ba.h5FilePrefix
    for i in 1:N
        fileName *= "$(chunkGlobalRange.start[i])-$(chunkGlobalRange.stop[i])_"
    end
    return joinpath(H5SBIGARRAY_DIRECTORY, "$(fileName[1:end-1]).h5")
end
function get_block_file_name(ba::H5sBigArray, idx::CartesianIndex)
    chunkID = index2blockid( idx, ba.blockSize )
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
    @show idxes
    @show ba.globalOffset
    idxes = map((x,y)-> x-y, idxes, ba.globalOffset)
    bufferGlobalRange = CartesianRange(idxes)

    baIter = BigArrayIterator(bufferGlobalRange, ba.blockSize)

    # temporal block as a buffer to reduce memory allocation
    @sync for (chunkID, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer) in baIter
        @async save_buffer( buf, get_block_file_name(ba, chunkID), ba, rangeInChunk, rangeInBuffer)
    end
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
            dataspace(ba.blockSize...),
            "chunk", ba.chunkSize,
            "blosc", 5)
    end
    dataSet[cartesianrange2unitrange(rangeInChunk)...] = 
        buf[cartesianrange2unitrange(rangeInBuffer)...]
    close(f)
end


function Base.setindex!{T,N}(dataSet::HDF5.HDF5Dataset, buf::Array{T,N},
                                rangeInChunk::CartesianRange{CartesianIndex{N}})
    ur = cartesianrange2unitrange(rangeInChunk)
    # @show ur
    dataSet[ur...] = buf
end

end # end of module: H5sBigArrays
