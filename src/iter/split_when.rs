pub struct ChunkWhen<'a, T, F>
where
    F: Fn(&T) -> bool,
{
    chunk: &'a [T],
    split_when: F,
}

impl<'a, T, F> ChunkWhen<'a, T, F>
where
    F: Fn(&T) -> bool,
{
    pub fn new(slice: &'a [T], split_when: F) -> Self {
        Self {
            chunk: slice,
            split_when,
        }
    }
}

impl<'a, T, F> Iterator for ChunkWhen<'a, T, F>
where
    F: Fn(&T) -> bool,
{
    type Item = &'a [T];

    fn next(&mut self) -> Option<Self::Item> {
        if self.chunk.is_empty() {
            None
        } else {
            match self.chunk.iter().position(|x| (self.split_when)(x)) {
                Some(index) => {
                    let slice = &self.chunk[..index];
                    self.chunk = &self.chunk[index + 1..];
                    Some(slice)
                }
                None => {
                    self.chunk = &[];
                    None
                }
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}
