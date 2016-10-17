# store small cuboids in AWS S3, download all the cuboids and cutout locally.
# The larger the cuboids size, the more redundency the download will be.
# The smaller the cuboids, the more impact on speed from the network latency.

"""
BigArray with backend of Google Cloud Storage
"""
module GCSBigArray

using ..BigArrays
using GoogleCloud
using GoogleCloud.Utils.Storage
using JSON

# include("../types.jl")
# include("../index.jl")
const DEFAULT_CONFIG_FILE = "config.json"
const DEFAULT_PREFIX = "cuboid_"
const DEFAULT_CUBOID_SIZE = (516, 516, 64)
const DEFAULT_GLOBAL_OFFSET = (0,0,0)

"""
definition of big array
"""
type GCSBigArray <: AbstractBigArray
    prefix          ::AbstractString
    globalOffset    ::NTuple{3, Int}
    cuboidSize      ::NTuple{3, Int}
    eltype          ::Type
    cartesianRange  ::CartesianRange
    compression     ::Symbol              # deflate || blosc
end

"""
handle vector type
"""
function GCSBigArray(   prefix      ::AbstractString,
                        globalOffset::Vector,
                        blockSize   ::Vector,
                        chunkSize   ::Vector,
                        eltype      ::Type,
                        cartesianRange  ::CartesianRange
                        compression     ::AbstractString)
    BigArray(   prefix,
                NTuple{3, Int}((globalOffset ...)),
                NTuple{3, Int}((cuboidSize ...)),
                eltype,
                cartesianRange,
                Symbol(compression))
end

"""
construct a BigArray from a dict
"""
function GCSBigArray( configDict::Dict{Symbol, Any} )
    if isa(configDict[:eltype], AbstractString)
        configDict[:eltype] = eval(parse( configDict[:eltype] ))
    end
    if isa(configDict[:cartesianRange], AbstractString)
        configDict[:cartesianRange] = eval(parse(configDict[:cartesianRange]))
    end
    BigArray(   configDict[:prefix],
                configDict[:globalOffset],
                configDict[:cuboidSize],
                configDict[:eltype],
                configDict[:cartesianRange],
                configDict[:compression] )
end

"""
construct from a register file, which defines file architecture
"""
function GCSBigArray(  dir::AbstractString;
                    prefix::AbstractString          = DEFAULT_PREFIX,
                    globalOffset::NTuple{3, Int}    = DEFAULT_GLOBAL_OFFSET,
                    cuboidSize::NTuple{3, Int}      = DEFAULT_CUBOID_SIZE,
                    eltype::Type                    = UInt8,
                    compression::Symbol             = :deflate)
    @assert ismatch(r"^gs://*", dir)
    configFile = joinpath(dir, DEFAULT_CONFIG_FILE)
    bkt, key = gspath2bkt_key(configFile)
    configString = storage(:Object, :get, bkt, key)
    if isa(configString, Dict) && haskey(config, :error)
        # the file is not exist, create one
        ba = BigArray(prefix, globalOffset, cuboidSize, eltype, compression)
        updateconfigfile(ba)
    else
        configDict = JSON.parse(configString, dicttype=Dict{Symbol, Any})
        ba = GCSBigArray( configDict )
    end
    ba
end


"""
transform bigarray to string
"""
function bigArray2dict(ba::GCSBigArray)
    d = Dict{Symbol, Any}()
    d[:prefix]          = ba.prefix
    d[:globalOffset]    = ba.globalOffset
    d[:cuboidSize]      = ba.cuboidSize
    d[:eltype]          = ba.eltype
    d[:cartesianRange]  = ba.cartesianRange
    d[:compression]     = ba.compression
    return d
end

function bigArray2string(ba::GCSBigArray)
    d = bigArray2dict(ba)
    @show d
    JSON.json(d)
end

"""
update the config.json file
"""
function updateconfigfile(ba::GCSBigArray)
    str = bigArray2string(ba)
    @show str
    configFile = joinpath(ba.dir, CONFIG_FILE)
    gsupload(configFile, str; content_type="text/html")
end

"""
element type of big array
"""
function Base.eltype(ba::BigArray)
    ba.eltype
end

"""
number of dimension
"""
function Base.ndims(ba::BigArray)
    length(ba.cuboidSize)
end

"""
bounding box of the whole volume
"""
function boundingbox(ba::GCSBigArray)
  x1 = Inf;   x2 = -Inf;
  y1 = Inf;   y2 = -Inf;
  z1 = Inf;   z2 = -Inf;
  # use bucket list to get file names and then analyse the file names

  # for file in readdir(GCSBigArray_DIRECTORY)
  #   if ishdf5(file)
  #       f = h5open(file)
  #       origin = f["origin"]
  #       sz = size(f[H5_DATASET_NAME])
  #       close(f)
  #       origin = fileName2origin(  )
  #       x1 = min(x1, origin[1])
  #       y1 = min(y1, origin[2])
  #       z1 = min(z1, origin[3])
  #       x2 = max(x2, origin[1]+sz[1]-1)
  #       y2 = max(y2, origin[2]+sz[2]-1)
  #       z2 = max(z2, origin[3]+sz[3]-1)
  #   end
  # end
  # (Int64(x1):Int64(x2), Int64(y1):Int64(y2), Int64(z1):Int64(z2))
end

"""
compute size from bounding box
"""
function Base.size(ba::GCSBigArray)
  bb = BoundingBox(ba)
  size(bb)
end

function Base.size(ba::GCSBigArray, i::Int)
  size(ba)[i]
end

function Base.show(ba::GCSBigArray)
  println("element type: $(eltype(ba))")
  println("size: $(size(ba))")
  println("bounding box: $(bbox(ba))")
  println("the data is in disk, not shown here.")
end

"""
extract chunk from a bigarray
only works for 3D now.
"""
function Base.getindex(ba::GCSBigArray, idxes::Union{UnitRange, Int, Colon}...)
    @show idxes
    # transform to originate from (0,0,0)
    globalOffset = ba.globalOffset
    xRange = idxes[1] - globalOffset[1]
    yRange = idxes[2] - globalOffset[2]
    zRange = idxes[3] - globalOffset[3]
    if length(idxes)==3
        idxes = (xRange, yRange, zRange)
    elseif length(idxes)==4
        idxes = (xRange, yRange, zRange, idxes[4])
    else
        error("only support 3D and 4D now, get $(length(idxes))")
    end

    @show globalOffset
    @show idxes
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
        channelNum = 0
        for file in readdir(GCSBigArray_DIRECTORY)
            h5FileName = joinpath(GCSBigArray_DIRECTORY, file)
            if ishdf5(h5FileName)
                f = h5open(h5FileName)
                channelNum = size(f[H5_DATASET_NAME])[4]
                close(f)
                break
            end
        end
        buf = zeros(eltype(ba), (sx,sy,sz, channelNum))
    end
    for giz in GlobalIndex(idxes[3], ba.blockSize[3])
        for giy in GlobalIndex(idxes[2], ba.blockSize[2])
            for gix in GlobalIndex(idxes[1], ba.blockSize[1])
                # get block id
                bidx, bidy, bidz = blockid((gix,giy,giz), ba.blockSize)
                # global coordinate
                globalOriginX = globalOffset[1] + (bidx-1) * ba.blockSize[1] + 1
                globalOriginY = globalOffset[2] + (bidy-1) * ba.blockSize[2] + 1
                globalOriginZ = globalOffset[3] + (bidz-1) * ba.blockSize[3] + 1
                # get hdf5 file name
                h5FileName = "$(ba.h5FilePrefix)$(globalOriginX)-$(globalOriginX+ba.blockSize[1]-1)_$(globalOriginY)-$(globalOriginY+ba.blockSize[2]-1)_$(globalOriginZ)-$(globalOriginZ+ba.blockSize[3]-1).h5"
                h5FileName = joinpath(GCSBigArray_DIRECTORY, h5FileName)
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
                else
                    warn("filled with zeros because file do not exist: $(h5FileName)")
                end
            end
        end
    end
    buf
end


"""
put small array to big array
"""
function Base.setindex!(ba::GCSBigArray, buf::Array, idxes::Union{UnitRange, Int, Colon}...)
    # transform to originate from (0,0,0)
    globalOffset = ba.globalOffset
    xRange = idxes[1] - globalOffset[1]
    yRange = idxes[2] - globalOffset[2]
    zRange = idxes[3] - globalOffset[3]
    if length(idxes)==3
        idxes = (xRange, yRange, zRange)
    elseif length(idxes)==4
        idxes = (xRange, yRange, zRange, idxes[4])
    else
        error("only support 3D and 4D now, get $(length(idxes))")
    end

    # only support 3D now
    @assert length(idxes[1]) == size(buf, 1)
    @assert length(idxes[2]) == size(buf, 2)
    @assert length(idxes[3]) == size(buf, 3)

    for giz in GlobalIndex(idxes[3], ba.blockSize[3])
        for giy in GlobalIndex(idxes[2], ba.blockSize[2])
            for gix in GlobalIndex(idxes[1], ba.blockSize[1])
                # get block id
                bidx, bidy, bidz = blockid((gix,giy,giz), ba.blockSize)
                # global coordinate
                globalOriginX = globalOffset[1] + (bidx-1) * ba.blockSize[1] + 1
                globalOriginY = globalOffset[2] + (bidy-1) * ba.blockSize[2] + 1
                globalOriginZ = globalOffset[3] + (bidz-1) * ba.blockSize[3] + 1
                # get hdf5 file name
                h5FileName = "$(ba.h5FilePrefix)$(globalOriginX)-$(globalOriginX+ba.blockSize[1]-1)_$(globalOriginY)-$(globalOriginY+ba.blockSize[2]-1)_$(globalOriginZ)-$(globalOriginZ+ba.blockSize[3]-1).h5"
                h5FileName = joinpath(GCSBigArray_DIRECTORY, h5FileName)
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
decode file name to origin coordinate
to-do: support negative coordinate.
"""
function fileName2origin( fileName::AbstractString )
    origin = zeros(Int, 3)
    secs = split(fileName, "_")
    origin[1] = parse( split(secs[2],"-")[1] )
    origin[2] = parse( split(secs[3],"-")[1] )
    origin[3] = parse( split(secs[4],"-")[1] )
    return origin
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
        # assign origin
        # f["origin"] = #fileName2origin( h5FileName )
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
        else
            dataSet = d_create(f, H5_DATASET_NAME, datatype(eltype(buf)),
                dataspace(ba.blockSize[1], ba.blockSize[2], ba.blockSize[3]),
                "chunk", (ba.chunkSize[1], ba.chunkSize[2], ba.chunkSize[3]))
        end
    end
    # @show blkix, blkiy, blkiz
    @show dataSet
    dataSet[blkix, blkiy, blkiz] = buf[bufix, bufiy, bufiz]
    close(f)
end

function save_buffer{T}(    buf::Array{T, 4}, h5FileName, ba,
                            blkix, blkiy, blkiz,
                            bufix, bufiy, bufiz)
    @assert ndims(buf)==4
    # the number of channels, it is 3 for affinity map, 5 or more for semantic segmentation
    channelNum = size(buf, 4)

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
                dataspace(ba.blockSize[1], ba.blockSize[2], ba.blockSize[3], channelNum),
                "chunk", (ba.chunkSize[1], ba.chunkSize[2], ba.chunkSize[3], channelNum),
                "shuffle", (), "deflate", 3)
        elseif ba.compression == :blosc
            dataSet = d_create(f, H5_DATASET_NAME, datatype(eltype(buf)),
                dataspace(ba.blockSize[1], ba.blockSize[2], ba.blockSize[3], channelNum),
                "chunk", (ba.chunkSize[1], ba.chunkSize[2], ba.chunkSize[3], channelNum),
                "blosc", 3)
        else
            dataSet = d_create(f, H5_DATASET_NAME, datatype(eltype(buf)),
                dataspace(ba.blockSize[1], ba.blockSize[2], ba.blockSize[3], channelNum),
                "chunk", (ba.chunkSize[1], ba.chunkSize[2], ba.chunkSize[3], channelNum))
        end
        dataSet[blkix, blkiy, blkiz, :] = buf[bufix, bufiy, bufiz, :]
        close(f)
    end

end

end # end of module: Cuboids
