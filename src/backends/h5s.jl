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
const DEFAULT_H5FILE_PREFIX = "block_"
const H5_DATASET_NAME = "img"
const DEFAULT_BLOCK_SIZE = (1024, 1024, 128)
const DEFAULT_CHUNK_SIZE = (256, 256, 32)
const DEFAULT_GLOBAL_OFFSET = (0,0,0)
const DEFAULT_RANGE         = CartesianRange(
        CartesianIndex((typemax(Int), typemax(Int), typemax(Int))),
        CartesianIndex((0,0,0)))

const DEFAULT_COMPRESSION = :deflate

export H5sBigArray, boundingbox

"""
definition of h5s big array
"""
type H5sBigArray <: AbstractBigArray
    h5FilePrefix  ::AbstractString
    globalOffset  ::Tuple
    blockSize     ::Tuple
    chunkSize     ::Tuple
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
    if isa(configDict[:blockSize], Vector)
        configDict[:blockSize] = (configDict[:blockSize]...)
    end
    if isa(configDict[:chunkSize], Vector)
        configDict[:chunkSize] = (configDict[:chunkSize]...)
    end
    configDict[:compression] = Symbol(configDict[:compression])
    H5sBigArray(    configDict[:h5FilePrefix],
                    configDict[:globalOffset],
                    configDict[:blockSize],
                    configDict[:chunkSize],
                    configDict[:compression] )

end
"""
construct from a register file, which defines file architecture
"""
function H5sBigArray{N}(   dir::AbstractString;
                        h5FilePrefix::AbstractString    = DEFAULT_H5FILE_PREFIX,
                        globalOffset::NTuple{N}         = DEFAULT_GLOBAL_OFFSET,
                        blockSize::NTuple{N}            = DEFAULT_BLOCK_SIZE,
                        chunkSize::NTuple{N}            = DEFAULT_CHUNK_SIZE,
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
        ba = H5sBigArray(h5FilePrefix, globalOffset, blockSize, chunkSize,
                            compression)
        updateconfigfile(ba)
    end
    ba
end


"""
transform bigarray to string
"""
function bigArray2dict(ba::H5sBigArray)
    d = Dict{Symbol, Any}()
    d[:h5FilePrefix] = ba.h5FilePrefix
    d[:globalOffset] = ba.globalOffset
    d[:blockSize] = ba.blockSize
    d[:chunkSize] = ba.chunkSize
    d[:compression] = ba.compression
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
    range = CartesianRange(
            CartesianIndex([typemax(Int) for i = 1:D]...),
            CartesianIndex([typemin(Int) for i = 1:D]...))
    @show H5SBIGARRAY_DIRECTORY
    for file in readdir(H5SBIGARRAY_DIRECTORY)
        if ishdf5(file)
            start = fileName2origin(file)
            f = h5open(file)
            sz = size(f[H5_DATASET_NAME])
            close(f)
            stop = map((x,y)->x+y-1, start,sz)
            range.start = CartesianIndex(map((x,y)->max(x,y), range.start, start))
            range.stop  = CartesianIndex(map((x,y)->min(x,y), range.stop,  stop))
        end
    end
    return range
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
        h5FileName *= "$(globalOrigin[i])-$(globalOrigin[i]+ba.blockSize[i]-1)_"
    end
    return joinpath(H5SBIGARRAY_DIRECTORY, "$(h5FileName[1:end-1]).h5")
end

"""
    h5read{N}(blockFileName::AbstractString,
                    H5_DATASET_NAME::AbstractString,
                    blockRange::CartesianRange{CartesianIndex{N}})

read h5 file using CartesianRange.
"""
function HDF5.h5read{N}(blockFileName::AbstractString,
                    H5_DATASET_NAME::AbstractString,
                    blockRange::CartesianRange{CartesianIndex{N}})
    blockIndexes = cartesian_range2unitrange( blockRange )
    h5read(blockFileName, H5_DATASET_NAME, blockIndexes)
end

"""
extract chunk from a bigarray
only works for 3D now.
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
    for (blockID, globalRange, blockRange, bufferRange) in baIter
        blockFileName = get_block_file_name(ba, blockID)
        info("read $(globalRange) from $(blockRange) of $(blockFileName) to buffer $(bufferRange) ...")

        # if have data fill with data,
        # if not, no need to change, keep as zero
        if isfile(blockFileName) && ishdf5(blockFileName)
            # assign data value, preserve existing value
            while true
                try
                    buf[bufferRange] = h5read(blockFileName, H5_DATASET_NAME,
                                                blockRange)
                    break
                catch
                    rethrow()
                    warn("open and read $blockFileName failed, will try 5 seconds later...")
                    sleep(5)
                end
            end
        else
            warn("filled with zeros because file do not exist: $(blockFileName)")
        end
    end
    buf
end

"""
get block file name from a
"""
function get_block_file_name{N}( ba::H5sBigArray, blockID::NTuple{N})
    blockGlobalRange = blockid2global_range( blockID, ba.blockSize )

    fileName = ba.h5FilePrefix
    for i in 1:N
        fileName *= "$(blockGlobalRange.start[i])-$(blockGlobalRange.stop[i])_"
    end
    return joinpath(H5SBIGARRAY_DIRECTORY, "$(fileName[1:end-1]).h5")
end
function get_block_file_name(ba::H5sBigArray, idx::CartesianIndex)
    blockID = index2blockid( idx, ba.blockSize )
    get_block_file_name(ba, blockID)
end

"""
put small array to big array
"""
function Base.setindex!{T,N}(ba::H5sBigArray, buf::Array{T,N}, idxes::Union{UnitRange, Int, Colon}...)
    @assert N == length(idxes)
    # clarify the Colon
    idxes = colon2unitRange(buf, idxes)
    # set bounding box
    # adjust_range!(ba, idxes)
    # updateconfigfile(ba)
    # transform to originate from (0,0,0)
    idxes = map((x,y)-> x-y, idxes, ba.globalOffset)
    bufferGlobalRange = CartesianRange(idxes)

    baIter = BigArrayIterator(bufferGlobalRange, ba.blockSize)

    # temporal block as a buffer to reduce memory allocation
    for (blockID, globalRange, blockRange, bufferRange) in baIter
        # refresh the temporal block
        # map((x,y)->tempBlock[x]=buf[y], blockRange, bufferRange)
        blockFileName = get_block_file_name(ba, blockID)
        info("save $(globalRange) from buffer $(bufferRange) to $(blockRange) of $(blockFileName) ...")
        while true
            try
                save_buffer( buf, blockFileName, ba, blockRange, bufferRange)
                info("save $(globalRange) from buffer $(bufferRange) to $(blockRange) of $(blockFileName) ...")
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
    fileName = replace(fileName, "-",  ":")
    fileName = replace(fileName, "_:", "_-")
    secs = split(fileName, "_")
    origin = zeros(Int, length(secs)-2)
    for i in 1:length(origin)
        origin[i] = parse( split(secs[i+1],":")[1] )
    end
    return origin
end

"""
save part of or whole buffer to one hdf5 file
"""
function save_buffer{T,N}(  buf::Array{T,N}, blockFileName::AbstractString,
                            ba::AbstractBigArray,
                            blockRange ::CartesianRange{CartesianIndex{N}},
                            bufferRange::CartesianRange{CartesianIndex{N}})
    if isfile(blockFileName) && ishdf5(blockFileName)
        f = h5open(blockFileName, "r+")
        dataSet = f[H5_DATASET_NAME]
        @assert eltype(f[H5_DATASET_NAME])==T
    else
        f = h5open(blockFileName, "w")
        # assign origin
        @show blockFileName
        # f["origin"] = fileName2origin( blockFileName )
        # assign values
        if ba.compression == :deflate
            dataSet = d_create(f, H5_DATASET_NAME, datatype(eltype(buf)),
                dataspace(ba.blockSize...),
                "chunk", (ba.chunkSize...),
                "shuffle", (), "deflate", 3)

        elseif ba.compression == :blosc
            dataSet = d_create(f, H5_DATASET_NAME, datatype(eltype(buf)),
                dataspace(ba.blockSize...),
                "chunk", (ba.chunkSize...),
                "blosc", 3)
        else
            dataSet = d_create(f, H5_DATASET_NAME, datatype(eltype(buf)),
                dataspace(ba.blockSize...),
                "chunk", (ba.chunkSize...))
        end
    end
    dataSet[blockRange] = buf[bufferRange]
    close(f)
end


function Base.setindex!{T,N}(dataSet::HDF5.HDF5Dataset, buf::Array{T,N},
                                blockRange::CartesianRange{CartesianIndex{N}})
    ur = cartesian_range2unitrange(blockRange)
    dataSet[ur...] = buf
end

end # end of module: H5sBigArrays
