$codes = {
 'A' => 0b0000,
 'C' => 0b0001,
 'G' => 0b0010,
 'T' => 0b0011,
 'N' => 0b0100,
 '0' => 0b1000,
 '1' => 0b1001,
 '2' => 0b1010,
 '3' => 0b1011,
 '.' => 0b1100 }
 
def code(y)
  x = y.chr.upcase
  if $codes.has_key?(x)
    return $codes[x]
  else
    return 0b0100
  end
end

strs = (0..255).map do |x| 
  "\t0x" + code(x).to_s(16).rjust(2,"0") + " #{x == 255 ? "" : ","} // " + ( (x.chr =~ /[[:print:]]/) ? x.chr :  "0x" + x.to_s(16) )
end

puts "byte[] encodingVector={\n"
puts strs.join("\n")
puts "};\n"
  