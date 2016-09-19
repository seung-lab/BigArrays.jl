module H5sBigArrays
using ..BigArrays
using HDF5
using JSON

# include("../types.jl")
# include("../index.jl")
const CONFIG_FILE = "config.json"
const H5_DATASET_NAME = "main"
const DEFAULT_BLOCK_SIZE = (2048, 2048, 256)
const DEFAULT_CHUNK_SIZE = (256, 256, 32)

export H5sBigArray, boundingbox

"""
definition of h5s big array
"""
type H5sBigArray <: AbstractBigArray
  dir             ::AbstractString
  blockSize       ::NTuple{3, Int}
  chunkSize       ::NTuple{3, Int}
  compression     ::Symbol              # deflate || blosc
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
function H5sBigArray(   dir::AbstractString,
                        blockSize::Vector,
                        chunkSize::Vector,
                        compression::AbstractString)
    H5sBigArray(dir,    NTuple{3, Int}((blockSize ...)),
                        NTuple{3, Int}((chunkSize ...)),
                        Symbol(compression))
end

"""
construct a H5sBigArray from a dict
"""
function H5sBigArray( configDict::Dict{Symbol, Any} )
    H5sBigArray(configDict[:dir],
                configDict[:blockSize],
                configDict[:chunkSize],
                configDict[:compression] )

end
"""
construct from a register file, which defines file architecture
"""
function H5sBigArray(   dir::AbstractString;
                        blockSize::NTuple{3, Int}   = DEFAULT_BLOCK_SIZE,
                        chunkSize::NTuple{3, Int}   = DEFAULT_CHUNK_SIZE,
                        compression::Symbol         = :deflate)
    configFile = joinpath(dir, CONFIG_FILE)
    if isfile(dir)
        warn("take this file as bigarray config file: $(dir)")
        # string format of config
        configDict = JSON.parsefile(dir, dicttype=Dict{Symbol, Any})
        @show configDict
        ba = H5sBigArray( configDict )
    elseif isdir(dir) && isfile(configFile)
        # string format of config
        configDict = JSON.parsefile(configFile, dicttype=Dict{Symbol, Any})
        @show configDict
        ba = H5sBigArray( configDict )
    else
        if !isdir(dir)
          mkdir(dir)
        end
        ba = H5sBigArray(dir, blockSize, chunkSize, compression)
        updateconfigfile(ba)
    end
    ba
end


"""
transform bigarray to string
"""
function bigArray2dict(ba::H5sBigArray)
    d = Dict{Symbol, Any}()
    d[:dir] = ba.dir
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
  configFile = joinpath(ba.dir, CONFIG_FILE)
  if !isdir(ba.dir)
    mkdir(ba.dir)
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
  files = readdir(ba.dir)
  for file in files
    h5FileName = joinpath(ba.dir, file)
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
  for file in readdir(ba.dir)
    fileName = joinpath(ba.dir, file)
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
  x1 = Inf;   x2 = -Inf;
  y1 = Inf;   y2 = -Inf;
  z1 = Inf;   z2 = -Inf;
  for file in readdir(ba.dir)
    if ishdf5(file)
      f = h5open(file)
      origin = f["origin"]
      sz = ndims(f[H5_DATASET_NAME])

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
  bb = BoundingBox(ba)
  size(bb)
end

function Base.size(ba::H5sBigArray, i::Int)
  size(ba)[i]
end

function Base.show(ba::H5sBigArray)
  println("element type: $(eltype(ba))")
  println("size: $(size(ba))")
  println("bounding box: $(bbox(ba))")
  println("the data is in disk, not shown here.")
end

"""
extract chunk from a bigarray
only works for 3D now.
"""
function Base.getindex(ba::H5sBigArray, idxes::Union{UnitRange, Int, Colon}...)
    # only support 3D image now, could support arbitrary dimensions in the future
    # allocate memory
    sx = length(idxes[1])
    sy = length(idxes[2])
    sz = length(idxes[3])
    # create buffer
    if ndims(ba) == 3
        buf = zeros(eltype(ba), (sx,sy,sz))
    else
        @assert ndims(ba)==4
        buf = zeros(eltype(ba), (sx,sy,sz,3))
    end
    for giz in GlobalIndex(idxes[3], ba.blockSize[3])
        for giy in GlobalIndex(idxes[2], ba.blockSize[2])
            for gix in GlobalIndex(idxes[1], ba.blockSize[1])
                # get block id
                bidx, bidy, bidz = blockid((gix,giy,giz), ba.blockSize)
                # get hdf5 file name
                h5FileName = joinpath(ba.dir, "chunk_$(bidx)_$(bidy)_$(bidz).h5")
                # if have data fill with data,
                # if not, no need to change, keep as zero
                if isfile(h5FileName) && ishdf5(h5FileName)
                    # compute index in hdf5
                    blkix, blkiy, blkiz = globalIndexes2blockIndexes((gix,giy,giz), ba.blockSize)
                    # compute index in buffer
                    bufix, bufiy, bufiz = globalIndexes2bufferIndexes((gix,giy,giz), idxes)
                    # assign data value, preserve existing value
                    info("read ($(gix), $giy, $giz) from ($(blkix), $(blkiy), $(blkiz)) of $(h5FileName) to buffer ($bufix, $bufiy, $bufiz)")
                    while true
                        try
                            if length(idxes)==3
                                buf[bufix, bufiy, bufiz] = h5read(h5FileName, H5_DATASET_NAME, (blkix,blkiy,blkiz))
                            else
                                @assert length(idxes)==4
                                @assert ndims(ba)==4
                                @show (blkix, blkiy, blkiz, :)
                                buf[bufix, bufiy, bufiz,:] = h5read(h5FileName, H5_DATASET_NAME, (blkix, blkiy, blkiz, :))
                            end
                            break
                        catch
                            rethrow()
                            warn("open and read $h5FileName failed, will try 5 seconds later...")
                            sleep(5)
                        end
                    end
                end
            end
        end
    end
    buf
end


"""
put small array to big array
"""
function Base.setindex!(ba::H5sBigArray, buf::Array, idxes::Union{UnitRange, Int, Colon}...)
    # only support 3D now
    @assert length(idxes[1]) == size(buf, 1)
    @assert length(idxes[2]) == size(buf, 2)
    @assert length(idxes[3]) == size(buf, 3)

    for giz in GlobalIndex(idxes[3], ba.blockSize[3])
        for giy in GlobalIndex(idxes[2], ba.blockSize[2])
            for gix in GlobalIndex(idxes[1], ba.blockSize[1])
                # get block id
                bidx, bidy, bidz = blockid((gix,giy,giz), ba.blockSize)
                # get hdf5 file name
                h5FileName = joinpath(ba.dir, "chunk_$(bidx)_$(bidy)_$(bidz).h5")
                @show h5FileName
                # compute index in hdf5
                blkix, blkiy, blkiz = globalIndexes2blockIndexes((gix,giy,giz), ba.blockSize)
                # compute index in buffer
                bufix, bufiy, bufiz = globalIndexes2bufferIndexes((gix,giy,giz), idxes)
                # put buffer subarray to hdf5, reserve existing values
                while true
                    try
                        save_buffer(buf, h5FileName, ba,
                                    blkix, blkiy, blkiz,
                                    bufix, bufiy, bufiz)
                        info("save ($gix, $giy, $giz) from buffer ($bufix, $bufiy, $bufiz) to ($blkix, $blkiy, $blkiz) of $(h5FileName)")
                        break
                    catch
                        rethrow()
                        warn("open and write $h5FileName failed, will try 5 seconds later...")
                        sleep(5)
                    end
                end
            end
        end
    end
end

"""
save part of or whole buffer to one hdf5 file
"""
function save_buffer{T}(    buf::Array{T, 3}, h5FileName, ba,
                            blkix, blkiy, blkiz,
                            bufix, bufiy, bufiz)
    if isfile(h5FileName) && ishdf5(h5FileName)
        f = h5open(h5FileName, "r+")
        dataSet = f[H5_DATASET_NAME]
        @assert eltype(f[H5_DATASET_NAME])==T
    else
        f = h5open(h5FileName, "w")
        # assign values
        if ba.compression == :deflate
            dataSet = d_create(f, H5_DATASET_NAME, datatype(eltype(buf)),
                dataspace(ba.blockSize[1], ba.blockSize[2], ba.blockSize[3]),
                "chunk", (ba.chunkSize[1], ba.chunkSize[2], ba.chunkSize[3]),
                "shuffle", (), "deflate", 3)

        elseif ba.compression == :blosc
            dataSet = d_create(f, H5_DATASET_NAME, datatype(eltype(buf)),
                dataspace(ba.blockSize[1], ba.blockSize[2], ba.blockSize[3]),
                "chunk", (ba.chunkSize[1], ba.chunkSize[2], ba.chunkSize[3]),
                "blosc", 3)
        end
    end
    # @show bufix, bufiy, bufiz
    # @show blkix, blkiy, blkiz
    @show dataSet
    dataSet[blkix, blkiy, blkiz] = buf[bufix, bufiy, bufiz]
    close(f)
end

function save_buffer{T}(    buf::Array{T, 4}, h5FileName, ba,
                            blkix, blkiy, blkiz,
                            bufix, bufiy, bufiz)
    @assert ndims(buf)==4 && size(buf,4)==3
    # @show blkix, blkiy, blkiz
    # @show bufix, bufiy, bufiz
    if isfile(h5FileName) && ishdf5(h5FileName)
        println("find an existing file: $(h5FileName)")
        f = h5open(h5FileName, "r+")
        @show f
        dataSet = f[H5_DATASET_NAME]
        @show dataSet
        @assert eltype(f[H5_DATASET_NAME])==T
        dataSet[blkix, blkiy, blkiz, :] = buf[bufix, bufiy, bufiz, :]
        close(f)
    else
        println("no such file, create one: $(h5FileName)")
        f = h5open(h5FileName, "w")
        @show f
        # assign values
        if ba.compression == :deflate
            dataSet = d_create(f, H5_DATASET_NAME, datatype(eltype(buf)),
                dataspace(ba.blockSize[1], ba.blockSize[2], ba.blockSize[3], 3),
                "chunk", (ba.chunkSize[1], ba.chunkSize[2], ba.chunkSize[3], 3),
                "shuffle", (), "deflate", 3)
        elseif ba.compression == :blosc
            dataSet = d_create(f, H5_DATASET_NAME, datatype(eltype(buf)),
                dataspace(ba.blockSize[1], ba.blockSize[2], ba.blockSize[3], 3),
                "chunk", (ba.chunkSize[1], ba.chunkSize[2], ba.chunkSize[3], 3),
                "blosc", 3)
        end
        dataSet[blkix, blkiy, blkiz, :] = buf[bufix, bufiy, bufiz, :]
        close(f)
    end

end

end # end of module: H5sBigArrays
