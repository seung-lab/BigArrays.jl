module H5sBigArrays
using ..BigArrays
using HDF5
using JSON

# include("../types.jl")
# include("../index.jl")
const CONFIG_FILE = "config.json"
const DEFAULT_H5FILE_PREFIX = "block_"
const H5_DATASET_NAME = "img"
const DEFAULT_BLOCK_SIZE = (2048, 2048, 256)
const DEFAULT_CHUNK_SIZE = (256, 256, 32)
const DEFAULT_GLOBAL_OFFSET = (0,0,0)

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
handle vector type
"""
function H5sBigArray(   h5FilePrefix    ::AbstractString,
                        globalOffset    ::Vector,
                        blockSize       ::Vector,
                        chunkSize       ::Vector,
                        compression     ::AbstractString)
    H5sBigArray(    h5FilePrefix,
                    (globalOffset ...),
                    (blockSize ...),
                    (chunkSize ...),
                    Symbol(compression))
end

"""
construct a H5sBigArray from a dict
"""
function H5sBigArray( configDict::Dict{Symbol, Any} )
    H5sBigArray(    configDict[:h5FilePrefix],
                    configDict[:globalOffset],
                    configDict[:blockSize],
                    configDict[:chunkSize],
                    configDict[:compression] )

end
"""
construct from a register file, which defines file architecture
"""
function H5sBigArray(   dir::AbstractString;
                        h5FilePrefix::AbstractString    = DEFAULT_H5FILE_PREFIX,
                        globalOffset::Tuple             = DEFAULT_GLOBAL_OFFSET,
                        blockSize::Tuple                = DEFAULT_BLOCK_SIZE,
                        chunkSize::Tuple                = DEFAULT_CHUNK_SIZE,
                        compression::Symbol             = :deflate)
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
        ba = H5sBigArray(h5FilePrefix, globalOffset, blockSize, chunkSize, compression)
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
    @show d
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
  @show str

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
    d = ndims(ba)
    start =
    x1 = Inf;   x2 = -Inf;
    y1 = Inf;   y2 = -Inf;
    z1 = Inf;   z2 = -Inf;
    for file in readdir(H5SBIGARRAY_DIRECTORY)
        if ishdf5(file)
            f = h5open(file)
            origin = f["origin"]
            sz = size(f[H5_DATASET_NAME])
            close(f)
            # origin = fileName2origin(  )
            x1 = min(x1, origin[1])
            y1 = min(y1, origin[2])
            z1 = min(z1, origin[3])
            x2 = max(x2, origin[1]+sz[1]-1)
            y2 = max(y2, origin[2]+sz[2]-1)
            z2 = max(z2, origin[3]+sz[3]-1)
        end
    end
    (Int64(x1):Int64(x2), Int64(y1):Int64(y2), Int64(z1):Int64(z2))
end

bbox(ba::H5sBigArray) = boundingbox(ba::H5sBigArray)

"""
compute size from bounding box
"""
function Base.size(ba::H5sBigArray)
    bb = boundingbox(ba)
    size(bb)
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
extract chunk from a bigarray
only works for 3D now.
"""
function Base.getindex(ba::H5sBigArray, idxes::Union{UnitRange, Int, Colon}...)
    @show idxes
    # clarify the Colon
    idxes = colon2unitRange(buf, idxes)
    # transform to originate from (0,0,0)
    idxes = [idxes...] .- [ba.globalOffset...]

    @show ba.globalOffset
    @show idxes
    # only support 3D image now, could support arbitrary dimensions in the future
    # allocate memory
    sz = map(length, idxes)
    buf = zeros(eltype(ba), sz)

    for gidxes in map(GlobalIndex, zip(idxes, ba.blockSize))
        # get block id
        bids = map(blockid, zip(gidxes, ba.blockSize))
        # global coordinate
        globalOrigin = [ba.globalOffset...] .+ ([bids...]-1).* ba.blockSize .+ 1
        # get hdf5 file name
        h5FileName = get_filename(ba, globalOrigin)
        # if have data fill with data,
        # if not, no need to change, keep as zero
        if isfile(h5FileName) && ishdf5(h5FileName)
            # compute index in hdf5
            blkidxes = globalIndexes2blockIndexes(gidxes, ba.blockSize)
            # compute index in buffer
            bufidxes = globalIndexes2bufferIndexes(gidxes, idxes)
            # assign data value, preserve existing value
            while true
                try
                    buf[bufidxes] = h5read(h5FileName, H5_DATASET_NAME, blkidxes)
                    break
                catch
                    rethrow()
                    warn("open and read $h5FileName failed, will try 5 seconds later...")
                    sleep(5)
                end
            end
        else
            warn("filled with zeros because file do not exist: $(h5FileName)")
        end
    end
    buf
end


"""
put small array to big array
"""
function Base.setindex!(ba::H5sBigArray, buf::Array, idxes::Union{UnitRange, Int, Colon}...)
    @assert ndims(buf) == length(idxes)
    # clarify the Colon
    idxes = colon2unitRange(buf, idxes)
    # set bounding box
    # blendchunk!(ba.boundingBox, buf, idxes)
    # transform to originate from (0,0,0)
    idxes = [idxes...] .- [ba.globalOffset...]

    for gidxes in map(x->GlobalIndex(x), zip(idxes, ba.blockSize))
        # get block id
        bids = map(blockid, zip(gidxes, ba.blockSize))
        # global coordinate
        globalOrigin = [ba.globalOffset...] .+ ([bids...].-1) .* [ba.blockSize...] .+ 1
        # get hdf5 file name
        h5FileName = get_filename(ba, globalOrigin)
        @show h5FileName
        # compute index in hdf5
        blkidxes = globalIndexes2blockIndexes(gidxes, ba.blockSize)
        # compute index in buffer
        bufidxes = globalIndexes2bufferIndexes(gidxes, idxes)
        # put buffer subarray to hdf5, reserve existing values
        while true
            try
                save_buffer(buf, h5FileName, ba, blkidxes, bufidxes)
                info("save $(gidxes) from buffer $(bufidxes) to $(blkidxes) of $(h5FileName)")
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
    secs = split(fileName, "_")
    origin = zeros(Int, length(secs)-2)
    for i in 1:length(origin)
        origin[i] = parse( split(secs[i+1],"-")[1] )
    end
    return origin
end

"""
save part of or whole buffer to one hdf5 file
"""
function save_buffer{T,D}(  buf::Array{T, D}, h5FileName::AbstractString,
                            ba::AbstractBigArray,
                            blkidxes::Union{Tuple, Vector},
                            bufidxes::Union{Tuple, Vector})
    @assert D==length(blkidxes)
    @assert D==length(bufidxes)
    if isfile(h5FileName) && ishdf5(h5FileName)
        f = h5open(h5FileName, "r+")
        dataSet = f[H5_DATASET_NAME]
        @assert eltype(f[H5_DATASET_NAME])==T
    else
        f = h5open(h5FileName, "w")
        # assign origin
        # f["origin"] = #fileName2origin( h5FileName )
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
    # @show blkix, blkiy, blkiz
    @show dataSet
    dataSet[blkidxes...] = buf[bufidxes...]
    close(f)
end

end # end of module: H5sBigArrays
