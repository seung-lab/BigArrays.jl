using LMDB

include("../../core/config.jl")
include("../bigarray.jl")
include("../boundingbox.jl")
include("../index.jl")

immutable LMDBBigArray <: AbstractBigArray
    dbpath::AbstractString
    chunksz::Vector
end

"""
default constructor
"""
function LMDBBigArray()
    fdb = "/tmp/lmdb"
    if !isdir(fdb)
        mkdir(fdb)
    end
    LMDBBigArray(fdb, [512,512,64])
end

"""
construct from a register file, which defines file architecture
"""
function H5sBigArray(fconfig::AbstractString)
    # string format of config
    strreg = readall(fconfig)
    config = JSON.parse(strreg, dicttype=Tconfig)
    config[:prefix] = joinpath(dirname(fconfig), basename(config[:prefix]))
    H5sBigArray(config[:prefix], config[:blocksz], config[:chunksz])
end

"""
element type of big array
"""
function Base.eltype(A::H5sBigArray)
    files = readdir(dirname(A.prefix))
    for file in files
        fname = joinpath(dirname(A.prefix), file)
        if ishdf5(fname)
            f = h5open(fname)
            ret = eltype(f["main"])
            close(f)
            return ret
        end
    end
end

"""
number of dimension
"""
function Base.ndims(A::H5sBigArray)
    for file in readdir(dirname(A.prefix))
        if ishdf5(file)
            f = h5open(file)
            ret = ndims(f["main"])
            close(f)
            return ret
        end
    end
end

"""
bounding box of the whole volume
"""
function Tbbox(A::H5sBigArray)
    x1 = Inf;   x2 = -Inf;
    y1 = Inf;   y2 = -Inf;
    z1 = Inf;   z2 = -Inf;
    for file in readdir(dirname(A.prefix))
        if ishdf5(file)
            f = h5open(file)
            origin = f["origin"]
            sz = ndims(f["main"])

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
bbox(A::H5sBigArray) = Tbbox(A)

"""
compute size from bounding box
"""
function Base.size(A::H5sBigArray)
    bb = Tbbox(A)
    size(bb)
end

function Base.size(A::H5sBigArray, i::Int)
    size(A)[i]
end

function Base.show(A::H5sBigArray)
    println("element type: $(eltype(A))")
    println("size: $(size(A))")
    println("bounding box: $(bbox(A))")
    println("the data is in disk, not shown here.")
end

"""
extract chunk from a bigarray
only works for 3D now.
"""
function Base.getindex(A::H5sBigArray, idx::Union{UnitRange, Int}...)
    # only support 3D image now, could support arbitrary dimensions in the future
    @assert length(idx) == 3
    # allocate memory
    sx = length(idx[1])
    sy = length(idx[2])
    sz = length(idx[3])
    # create buffer
    buf = zeros(eltype(A), (sx,sy,sz))
    for giz in TGIdxs(idx[3], A.blocksz[3])
        for giy in TGIdxs(idx[2], A.blocksz[2])
            for gix in TGIdxs(idx[1], A.blocksz[1])
                # get block id
                bidx, bidy, bidz = blockid((gix,giy,giz), A.blocksz)
                # get hdf5 file name
                fname = "$(A.prefix)$(bidx)_$(bidy)_$(bidz).h5"
                # if have data fill with data,
                # if not, no need to change, keep as zero
                if isfile(fname) && ishdf5(fname)
                    # compute index in hdf5
                    blkix, blkiy, blkiz = gidx2blkidx((gix,giy,giz), A.blocksz)
                    # compute index in buffer
                    bufix, bufiy, bufiz = gidx2bufidx((gix,giy,giz), idx)
                    # assign data value, preserve existing value
                    info("read ($(gix), $giy, $giz) from ($(blkix), $(blkiy), $(blkiz)) of $(fname) to buffer ($bufix, $bufiy, $bufiz)")
                    buf[bufix, bufiy, bufiz] = h5read(fname, "main", (blkix,blkiy,blkiz))
                end
            end
        end
    end
    buf
end


"""
put small array to big array
"""
function Base.setindex!(A::H5sBigArray, buf::Array, idx::Union{UnitRange, Int}...)
    # only support 3D now
    @assert length(idx)==3
    @assert length(idx[1]) == size(buf, 1)
    @assert length(idx[2]) == size(buf, 2)
    @assert length(idx[3]) == size(buf, 3)

    for giz in TGIdxs(idx[3], A.blocksz[3])
        for giy in TGIdxs(idx[2], A.blocksz[2])
            for gix in TGIdxs(idx[1], A.blocksz[1])
                # get block id
                bidx, bidy, bidz = blockid((gix,giy,giz), A.blocksz)
                # get hdf5 file name
                fname = "$(A.prefix)$(bidx)_$(bidy)_$(bidz).h5"
                # compute index in hdf5
                blkix, blkiy, blkiz = gidx2blkidx((gix,giy,giz), A.blocksz)
                # compute index in buffer
                bufix, bufiy, bufiz = gidx2bufidx((gix,giy,giz), idx)
                # put buffer subarray to hdf5, reserve existing values
                if isfile(fname) && ishdf5(fname)
                    f = h5open(fname, "r+")
                else
                    f = h5open(fname, "w")
                end
                if exists(f, "main")
                    dset = f["main"]
                    @assert eltype(dset)==eltype(buf)
                else
                    dset = d_create(f, "main", datatype(eltype(buf)),
                            dataspace(A.blocksz[1], A.blocksz[2], A.blocksz[3]),
                            "chunk", (A.chunksz[1],A.chunksz[2],A.chunksz[3]),
                            "shuffle", (), "deflate", 3)
                end
                # @show idx
                info("save ($(gix), $giy, $giz) from buffer ($bufix, $bufiy, $bufiz) to ($(blkix), $(blkiy), $(blkiz)) of $(fname)")
                # @show bufix, bufiy, bufiz
                # @show blkix, blkiy, blkiz
                dset[blkix, blkiy, blkiz] = buf[bufix, bufiy, bufiz]
                close(f)
            end
        end
    end
end
