using BigArrays
using BigArrays.AlignedBigArrays

registerFile = "/usr/people/jingpeng/seungmount/research/Julimaps/datasets/piriform/4_aligned/registry_aligned.txt"
ba = AlignedBigArray(registerFile)
img = ba[3001:3300, 2001:2400, 101:110]

include(joinpath(Pkg.dir(), "EMIRT/plugins/show.jl"))

imgc, imgslice = show(img)

#If we are not in a REPL
if (!isinteractive())

    # Create a condition object
    c = Condition()

    # Get the main window (A Tk toplevel object)
    win = toplevel(imgc)

    # Notify the condition object when the window closes
    bind(win, "<Destroy>", e->notify(c))

    # Wait for the notification before proceeding ...
    wait(c)
end
