module Context

export BigArrayContext

abstract AbstractBigArraysContext <: Any

type BigArrayContext <: AbstractBigArraysContext
    path        ::AbstractString
    chunkSize   ::Tuple{Int}
    backend     ::Symbol
    # compression ::Symbol
end



end # end of module
