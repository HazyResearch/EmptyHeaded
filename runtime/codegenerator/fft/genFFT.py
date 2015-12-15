import os
import code_blocks


class GenFFT(object):

    def __init__(self, b, m, verbose=False):
        self.b = b
        self.m = m
        self.n = b**m
        self.verbose = verbose

    def get_bag_code(self, bag_num):
        """
        Bag n will contain attributes
        xn, xn-1, ..., x0, y0, ..., y(m-n-2) [y(m-n-1) aggregated out]
        Level i iterates over x(n-i) for i <= n
                              y(i-n-1) for i > n

        Each bag only cares about factors which touch the new x
            Bag 0 joins with the x0,y* relations
            Bag n joins with the xn,y0, ... xn,y(m-n-1)
            Bag (m-1) joins with x(m-1),y0
        :param bag_num:
        :return:
        """
        # start with either the input or the output of the previous bag
        prev_bag_num = bag_num - 1
        if prev_bag_num < 0:
            prev_bag_num = "i"

        # code = blocks + reverse(rev_blocks_end)
        # reversed end blocks are useful for closing out foreach loops
        blocks = []
        rev_blocks_end = []

        # Declare builders and iterators
        blocks.append(code_blocks.declare_p_builder(bag_num))
        blocks.append(code_blocks.declare_p_bag_iterator(prev_bag_num))
        # the index of the y we are aggregating away
        agg_index = self.m - bag_num - 1
        for i in range(agg_index+1):
            blocks.append(
                code_blocks.declare_p_factor_iterator(bag_num, i)
            )
        blocks.append(code_blocks.declare_annots(self.m))

        # Outermost parallel foreach
        # Loops over x_{bag_num}
        var = "x{}".format(bag_num)
        blocks.append(code_blocks.comment_var(var))
        blocks.append(code_blocks.build_base(bag_num))
        rev_blocks_end.append('});')

        # Thread local builders and iterators
        blocks.append(code_blocks.declare_builder(bag_num))
        blocks.append(code_blocks.declare_bag_iterator(prev_bag_num))
        for i in range(agg_index+1):
            blocks.append(code_blocks.declare_factor_iterator(bag_num, i))
            blocks.append(code_blocks.factor_next_block(bag_num, i))

        # inside the par-foreach is level 0
        # Each level is responsible for
        # - building and allocating a block
        # - Looping through block
        # - Updating iterators
        for level in range(self.m - 1):
            # build the set for the next loop
            newlevel = level + 1
            xi = bag_num - newlevel
            yi = newlevel - bag_num - 1
            isx = (newlevel <= bag_num)
            newvar = "x{}".format(xi) if isx else "y{}".format(yi)

            blocks.append(code_blocks.comment_var(newvar))
            blocks.append(code_blocks.bag_build_set(bag_num, prev_bag_num, level))
            blocks.append(code_blocks.bag_set_level(bag_num, var))
            if level < self.m - 2:
                blocks.append(code_blocks.bag_allocate_next(bag_num))
            else:
                blocks.append(code_blocks.bag_allocate_annot(bag_num))

            var = newvar

            # loop
            blocks.append(code_blocks.bag_foreach(bag_num, var))
            rev_blocks_end.append('});')

            # Update partial annotation
            if not isx and bag_num + yi < self.m:
                blocks.append(code_blocks.mult_factor_annot(bag_num, yi))
            blocks.append(code_blocks.bag_next_block(prev_bag_num, level, var))

        blocks.append(code_blocks.zero_sum_annot)

        # Innermost aggregation loop
        aggvar = "y{}".format(agg_index)
        blocks.append(code_blocks.comment_var(aggvar))
        blocks.append(code_blocks.bag_build_agg_set(bag_num, prev_bag_num, self.m-1))
        blocks.append(code_blocks.bag_agg_foreach(bag_num, aggvar))
        blocks.append(code_blocks.mult_factor_annot(bag_num, agg_index))
        blocks.append(code_blocks.mult_annot_bag(agg_index, prev_bag_num, self.m))
        blocks.append('});')

        blocks.append(code_blocks.bag_set_annot(bag_num, var))

        rev_blocks_end.reverse()
        return "\n".join(blocks) + "\n".join(rev_blocks_end)

    def get_code(self):
        body = ""
        for bag_num in range(self.m):
            body += (
                code_blocks.declare_bag(bag_num) +
                "{" +
                self.get_bag_code(bag_num))
            if self.verbose or bag_num == self.m - 1:
                body += "\n" + code_blocks.bag_print(bag_num)

            body += "\n" + "}"
        return "\n".join([
            code_blocks.headers,
            code_blocks.prologue(self.b, self.m, self.n),
            code_blocks.start_timer("setup"),
            code_blocks.setup,
            code_blocks.end_timer("setup"),
            code_blocks.start_timer("fft"),
            body,
            code_blocks.end_timer("fft"),
            code_blocks.epilogue,
        ])

    def get_filename(self):
        filename = "{home}/storage_engine/apps/fft_gen_{b}_{m}.cpp".format(
            home=os.environ["EMPTYHEADED_HOME"],
            b=self.b,
            m=self.m,
        )
        return filename

    def write_code(self, code_str):
        os.system("mkdir -p "+os.environ["EMPTYHEADED_HOME"]+"/storage_engine/generated")
        filename = self.get_filename()
        with open(filename, "w") as cpp_file:
            cpp_file.write(code_str)
        os.system("clang-format -style=llvm -i "+filename)

    def gen(self):
        self.write_code(self.get_code())
        return self.get_filename()
