module AlignedBigArrays
using ..BigArrays
using HDF5

include("../types.jl")

const WAIVER_ID = 1

export AlignedBigArray

# register item of one section / hdf5 file
typealias Tsecreg Dict{Symbol, Union{AbstractString, Int}}
# the whole register records filename, xstart, ystart, xdim, ydim
typealias Tregister Dict{Tuple{Int, Int}, Tsecreg}

type AlignedBigArray <: AbstractBigArray
    register::Tregister
end

"""
construct from a register file,
which was the final output of registration pipeline in seunglab
"""
function AlignedBigArray(fregister::AbstractString)
    register = Tregister()
    f = open(fregister)
    lines = readlines(f)
    close(f)
    for line in lines
        # initialize the registration of a section image
        d = Tsecreg()
        fname, xoff, yoff, xdim, ydim, tf = split(line)
        z = parse(split(split(fname,'_')[1], ',')[2]) + 1
        waiverID = parse(split(split(fname,'_')[1], ',')[1])
        d[:fname] = joinpath(dirname(fregister), fname * ".h5")
        d[:xoff] = parse(xoff)
        d[:yoff] = parse(yoff)
        d[:zoff] = z-1
        d[:xdim] = parse(xdim)
        d[:ydim] = parse(ydim)
        d[:zdim] = 1
        register[(waiverID, z)] = d
    end
    AlignedBigArray(register)
end

"""
specialized for UInt8 raw image data type
"""
function Base.eltype(A::AlignedBigArray)
    for key in keys(A.register)
        fname = A.register[key][:fname]
        if isfile(fname) && ishdf5(fname)
            f = h5open(fname)
            ret = eltype(f["img"])
            close(f)
            return ret
        end
    end
end

"""
number of dimension
"""
function Base.ndims(A::AlignedBigArray)
    for key in keys(A.register)
        fname = A.register[key][:fname]
        if isfile(fname) && ishdf5(fname)
            f = h5open(fname)
            ret = ndims(f["img"])
            close(f)
            # don't forget the z dimension!
            return ret+1
        end
    end
end

"""
bounding box of the whole volume
"""
function Tbbox(A::AlignedBigArray)
    x1 = Inf;   x2 = -Inf;
    y1 = Inf;   y2 = -Inf;
    z1 = Inf;   z2 = -Inf;
    for d in values( A.register )
        x1 = min(x1, d[:xoff] + 1)
        y1 = min(y1, d[:yoff] + 1)
        z1 = min(z1, d[:zoff] + 1)
        x2 = max(x2, d[:xoff] + d[:xdim])
        y2 = max(y2, d[:xoff] + d[:ydim])
        z2 = max(z2, d[:zoff] + d[:zdim])
    end
    (Int64(x1):Int64(x2), Int64(y1):Int64(y2), Int64(z1):Int64(z2))
end
bbox(A::AlignedBigArray) = Tbbox(A)

"""
compute size from bounding box
"""
function Base.size(A::AlignedBigArray)
    bb = Tbbox(A)
    size(bb)
end

function Base.size(A::AlignedBigArray, i::Int)
    size(A)[i]
end

function Base.show(A::AlignedBigArray)
    println("type: $(typeof(A))")
    println("size: $(size(A))")
    println("bounding box: $(bbox(A))")
    println("the data is in disk, can not ")
end

"""
extract chunk from a bigarray
only works for 3D now.
"""
function Base.getindex(A::AlignedBigArray, idxes::Union{UnitRange, Int, Colon}...)
    # only support 3D image now, could support arbitrary dimensions in the future
    @assert length(idxes) == 3
    # allocate memory
    sx = length(idxes[1])
    sy = length(idxes[2])
    sz = length(idxes[3])
    # create buffer
    buf = zeros(UInt8, (sx,sy,sz))
    @show idxes
    for globalZ in idxes[3]
        key = (WAIVER_ID,globalZ)
        fname = A.register[key][:fname]
        xidx = idxes[1] - A.register[key][:xoff]
        yidx = idxes[2] - A.register[key][:yoff]
        zidx = globalZ  - A.register[key][:zoff]
        info("fname: $(basename(fname)), xidx: $(xidx), yidx: $(yidx), zidx: $(zidx)")
        @assert xidx.start > 0
        @assert yidx.start > 0
        @assert zidx > 0
        if ishdf5(fname)
            buf[:,:,zidx] = h5read(fname, "img", (xidx, yidx))
        else
            warn("no hdf5 file: $(fname) for section $(zidx), filled with zero")
        end
    end
    buf
end

end # end of module AlignedBackend
