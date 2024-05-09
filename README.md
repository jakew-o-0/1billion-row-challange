# Trying to parse 1 billion rows as fast as I can
the the challange is here: but the jist is to find the min, mean and max of weather stations within a generated measurements.txt file. To run this go to the 1brc git hub repo and go through the process of generating the measurements.txt file. Put the file in the root dir of this project and use the make file.
## Optimisations made
### Custom hash-map implementation
- the keys can then be bytes, avoiding the cost of casting to a string.
- using linear probing instead of a linked list is faster beacuse there will be a maximum of 10,000 buckets in the map
- using a faster hashing algorithm (xxhash)
### implementing std lib functions
the std lib covers a lot of edge cases that this project does not need. these methods include:
- replacing the bufio scanner with handling byte buffers manually and parsing those to get lines from them and to ensure only full lines are in the buffers.
- replacing bytes.split() with my own implamentation
- instead of using floadting point numbers fixed-point numbers where used instead. this is less combersome because all inputs and outputs will be to 1dp so by not using floating points we can gain some speed.
### concurrency
lastly using go's concurency model to parse chunks at the same time and using a channel to fan-in that data into a single map. 
