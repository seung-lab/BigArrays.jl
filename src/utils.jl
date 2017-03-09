module Utils

export fileName2origin, key2symbol

"""
decode file name to origin coordinate
to-do: support negative coordinate.
"""
function fileName2origin( fileName::AbstractString; prefix = "block_" )
    # @show fileName
    fileName = replace(fileName, prefix, "")
    fileName = replace(fileName, ".h5", "")
    fileName = replace(fileName, "-",  ":")
    fileName = replace(fileName, "_:", "_-")
    secs = split(fileName, "_")
    origin = zeros(Int, length(secs))
    for i in 1:length(origin)
        origin[i] = parse( split(secs[i],":")[1] )
    end
    return origin
end




end # end of module
