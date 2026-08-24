# Reading the gauge over serial

The gauge speaks a line protocol at 9600 baud and the only thing it will tell you is the current reading in millimetres, which is enough. Open the port and read.

```rust
fn read(port: &mut Serial) -> io::Result<i32> {
    let mut line = String::new();
    port.read_line(&mut line)?;
    line.trim().parse().map_err(io::Error::other)
}
```

On the shell side, use `stty` to set the port up first, and note that the inline example below contains a backtick so the fence has to be longer.

```
stty -F /dev/ttyUSB0 9600 raw
cat /dev/ttyUSB0 | while read -r mm; do echo "`date -Is` $mm"; done
```

A code span with a backtick in it: `` echo `date` ``, and one without: `stty -a`.