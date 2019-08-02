const TASK_NUM = 8
const CHUNK_CHANNEL_SIZE = 2

include("multithreads.jl")
#include("multiprocesses.jl")
include("sequential.jl")
#include("sharedarray.jl")
include("taskthreads.jl")