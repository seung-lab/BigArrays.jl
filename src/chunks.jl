export Chunks

typealias Coordinate Vector{Int}
typealias Size Vector{Int}

immutable Chunks
    ba::AbstractBigArray
    # where do we start in the bigarray
    origin::Coordinate
    # dimension of each chunk
    chunkSize::Size
    # overlap of each chunk
    overlap::Size
    # grid of chunks. let's say gridsz=(2,2,2), will produce 16 chunks
    gridsz::Size
    # voxel size
    voxelSize::Size
end

function Chunks(ba::AbstractBigArray, origin::Coordinate=[0,0,0],
                chunkSize::Size=[1024,1024,10], overlap::Size=[0,0,0],
                gridsz::Size=[1,1,1], voxelSize::Size=[1,1,1])
    Chunks(ba, origin, chunkSize, overlap, gridsz, voxelSize)
end
# iteration functions
# grid index as state, start from the first grid
Base.start(chks::Chunks) = Vector{UInt32}([1,1,1])
Base.done(chks::Chunks, grididx) = grididx[1]>chks.gridsz[1]

function Base.next(chks::Chunks, grididx::Vector)
    # get current chunk_
    step = chks.chunkSize .- chks.overlap
    start = chks.origin + (grididx-1) .* step
    stop = start + chks.chunkSize - 1
    arr = chks.ba[start[1]:stop[1], start[2]:stop[2], start[3]:stop[3]]
    chk = Chunk(arr, start, chks.voxelSize)

    # next grid index
    if grididx[1] < chks.gridsz[1]
        nextGridIndex = [grididx[1]+1, grididx[2], grididx[3]]
    elseif grididx[2] < chks.gridsz[2]
        nextGridIndex = [1, grididx[2]+1, grididx[3]]
    elseif grididx[3] < chks.gridsz[3]
        nextGridIndex = [1,1, grididx[3]+1]
    else
        nextGridIndex = grididx .+ [1,0,0]
    end
    return chk, nextGridIndex
end
