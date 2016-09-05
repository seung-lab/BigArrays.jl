module AlignedBigArrays
using ..BigArray
using HDF5

include("../types.jl")

export AlignedBigArray

# register item of one section / hdf5 file
typealias Tsecreg Dict{Symbol, Union{AbstractString, Int}}
# the whole register records filename, xstart, ystart, xdim, ydim
typealias Tregister Vector{Tsecreg}

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
        d[:fname] = joinpath(dirname(fregister), fname * ".h5")
        d[:xoff] = parse(xoff)
        d[:yoff] = parse(yoff)
        d[:xdim] = parse(xdim)
        d[:ydim] = parse(ydim)
        push!(register, d)
    end
    AlignedBigArray(register)
end

"""
specialized for UInt8 raw image data type
"""
function Base.eltype(A::AlignedBigArray)
    for z in 1:length(A.register)
        fname = A.register[z][:fname]
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
    for z in 1:length(A.register)
        fname = A.register[z][:fname]
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
    zidx = 1:length(A.register)
    x1 = Inf;   x2 = -Inf;
    y1 = Inf;   y2 = -Inf;
    for d in A.register
        x1 = min(x1, d[:xoff] +1)
        y1 = min(y1, d[:yoff] +1)
        x2 = max(x2, d[:xoff] + d[:xdim])
        y2 = max(y2, d[:xoff] + d[:ydim])
    end
    (Int64(x1):Int64(x2), Int64(y1):Int64(y2), zidx)
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
function Base.getindex(A::AlignedBigArray, idx::Union{UnitRange, Int, Colon}...)
    # only support 3D image now, could support arbitrary dimensions in the future
    @assert length(idx) == 3
    # allocate memory
    sx = length(idx[1])
    sy = length(idx[2])
    sz = length(idx[3])
    # create buffer
    buf = zeros(UInt8, (sx,sy,sz))
    for z in idx[3]
        fname = A.register[z][:fname]
        xidx = idx[1] - A.register[z][:xoff]
        yidx = idx[2] - A.register[z][:yoff]
        zidx = z - first(idx[3]) + 1
        info("fname: $(basename(fname)), xidx: $(xidx), yidx: $(yidx), zidx: $(zidx)")
        @assert xidx.start>0
        @assert yidx.start>0
        @assert zidx>0
        if ishdf5(fname)
            buf[:,:,zidx] = h5read(fname, "img", (xidx, yidx))
        else
            warn("no hdf5 file: $(fname) for section $(z), filled with zero")
        end
    end
    buf
end

end # end of module AlignedBackend
