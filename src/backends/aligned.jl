module AlignedBigArrays
using ..BigArrays
using HDF5

include("../types.jl")

const WAIVER_ID = 1
const H5_DATASET_NAME = "img"
const H5_DATASET_ELEMENT_TYPE = UInt8
const DATASET_NDIMS = 3

export AlignedBigArray, boundingbox

# register item of one section / hdf5 file
typealias Tsecreg Dict{Symbol, Union{AbstractString, Int}}
# the whole register records filename, xstart, ystart, xdim, ydim
typealias Tregister Dict{Int, Tsecreg}

type AlignedBigArray <: AbstractBigArray
    register::Tregister
end

"""
construct from a register file,
which was the final output of registration pipeline in seunglab
"""
function AlignedBigArray(fregister::AbstractString)
    f = open(fregister)
    lines = readlines(f)
    close(f)
    register = Tregister()
    # sizehint!(Tregister, length(lines))
    z = 0
    for i in eachindex(lines)
        z += 1
        # initialize the registration of a section image
        d = Tsecreg()
        line = lines[i]
        if length(split(line)) == 7
            registerFile, tmpZero, xoff, yoff, xdim, ydim, tf = split(line)
        elseif length(split(line))==6
            registerFile, xoff, yoff, xdim, ydim, tf = split(line)
        else
            error("unsupported format of register file: $(line)")
        end
        d[:secIDinWaver] = parse(split(split(registerFile,'_')[1], ',')[2])
        d[:waiverID] = parse(split(split(registerFile,'_')[1], ',')[1])
        d[:registerFile] = joinpath(dirname(fregister), registerFile * ".h5")
        d[:xoff] = parse(xoff)
        d[:yoff] = parse(yoff)
        d[:zoff] = z-1
        d[:xdim] = parse(xdim)
        d[:ydim] = parse(ydim)
        d[:zdim] = 1
        register[z] = d
    end
    AlignedBigArray(register)
end

"""
specialized for UInt8 raw image data type
"""
function Base.eltype(A::AlignedBigArray)
    return H5_DATASET_ELEMENT_TYPE
    # for key in keys(A.register)
    #     registerFile = A.register[key][:registerFile]
    #     if isfile(registerFile) && ishdf5(registerFile)
    #         f = h5open(registerFile)
    #         ret = eltype(f[])
    #         close(f)
    #         return ret
    #     end
    # end
end

"""
number of dimension
"""
function Base.ndims(A::AlignedBigArray)
    return DATASET_NDIMS
    # for key in keys(A.register)
    #     registerFile = A.register[key][:registerFile]
    #     if isfile(registerFile) && ishdf5(registerFile)
    #         f = h5open(registerFile)
    #         ret = ndims(f[H5_DATASET_NAME])
    #         close(f)
    #         # don't forget the z dimension!
    #         return ret+1
    #     end
    # end
end

"""
bounding box of the whole volume
"""
function boundingbox(A::AlignedBigArray)
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
    println("bounding box: $(bbox(A))")
    println("the data is in disk, can not ")
end

"""
read out part of the image from HDF5 file
"""
function read_subimage!(buf,
                        registerFile::AbstractString,
                        sizeX::Integer,
                        sizeY::Integer,
                        xidx::Union{Int, UnitRange},
                        yidx::Union{Int, UnitRange},
                        zidx::Integer)
    @assert ishdf5(registerFile)

    while true
        try
            # the explicit coordinate range
            x1 = max(1, first(xidx));   x2 = min(sizeX, last(xidx));
            y1 = max(1, first(yidx));   y2 = min(sizeY, last(yidx));
            if x1>x2 || y1>y2
                warn("no overlaping region in this section: $(registerFile)")
            else
                # index in buffer
                bufx1 = x1 - first(xidx) + 1;
                bufx2 = x2 - first(xidx) + 1;
                bufy1 = y1 - first(yidx) + 1;
                bufy2 = y2 - first(yidx) + 1;
                buf[bufx1:bufx2, bufy1:bufy2, zidx] = h5read(registerFile, H5_DATASET_NAME, (x1:x2, y1:y2))
            end
            return
        catch
            rethrow()
            sleep(2)
            warn("file was opened, wait for 2 seconds and try again.")
            warn("file name: $registerFile")
        end
    end
end

"""
extract chunk from a bigarray
only works for 3D now.
"""
function Base.getindex(A::AlignedBigArray, idxes::Union{UnitRange, Int}...)
    # only support 3D image now, could support arbitrary dimensions in the future
    @assert length(idxes) == 3
    # allocate memory
    sx = length(idxes[1])
    sy = length(idxes[2])
    sz = length(idxes[3])
    # create buffer
    buf = zeros(H5_DATASET_ELEMENT_TYPE, (sx,sy,sz))
    @show idxes
    for z in idxes[3]
        if haskey(A.register, z)
            registerFile = A.register[z][:registerFile]
            xidx = idxes[1] - A.register[z][:xoff]
            yidx = idxes[2] - A.register[z][:yoff]
            zidx = z  - first(idxes[3]) + 1
            # println("registerFile: $(basename(registerFile)), xidx: $(xidx), yidx: $(yidx), zidx: $(zidx)")
            read_subimage!( buf,
                            registerFile,
                            A.register[z][:xdim],
                            A.register[z][:ydim],
                            xidx, yidx, zidx)
        else
            warn("section file not exist: $(z)")
        end
    end
    buf
end

end # end of module AlignedBackend
