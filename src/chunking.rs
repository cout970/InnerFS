// This is a content-defined chunking algorithm, which means that the chunks are
// determined by the content of the data, rather than a fixed size.
//
// The algorithm is based on the Rabin-Karp algorithm, which uses a rolling hash
// to efficiently find substrings in a string.
//
// The chunks have an average size of 1MB, but can vary significantly depending on the data.
// Since the split is content-defined, a single change in the data will not affect the surrounding chunks.
//
// Inspired by:
// https://moinakg.wordpress.com/2013/06/22/high-performance-content-defined-chunking/
pub fn split_in_chunks(bytes: &[u8]) -> Vec<&[u8]> {
    const PRIME: u64 = 16777619u64;
    const WINDOW_SIZE: usize = 16;
    const MIN_CHUNK_SIZE: usize = 1024; // 1KB
    const MAX_CHUNK_SIZE: usize = 2 * 1024 * 1024; // 2MB
    const MASK: u64 = 0xFFFFF;

    // Early return for small inputs
    if bytes.len() < WINDOW_SIZE || bytes.len() < MIN_CHUNK_SIZE {
        return vec![bytes];
    }

    // Pre-calculate PRIME^(WINDOW_SIZE-1) for the rolling hash
    let mut highest_power = 1u64;
    for _ in 0..WINDOW_SIZE - 1 {
        highest_power = highest_power.wrapping_mul(PRIME);
    }

    // Estimate capacity to reduce reallocations
    // Average chunk size is 1MB, for a 10MB file, it creates around 5 to 15 chunks
    let estimated_chunks = ((bytes.len() / (MAX_CHUNK_SIZE / 2)) * 15 / 10).max(1);
    let mut res = Vec::with_capacity(estimated_chunks);

    let mut last_cut = 0;

    // Calculate initial hash for the first window
    let mut hash = 0u64;
    for i in 0..WINDOW_SIZE {
        hash = hash.wrapping_mul(PRIME).wrapping_add(bytes[i] as u64);
    }

    // Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.

    // Use rolling hash for the rest of the bytes
    for i in WINDOW_SIZE..bytes.len() {
        // Remove contribution of oldest byte
        hash = hash.wrapping_sub(highest_power.wrapping_mul(bytes[i - WINDOW_SIZE] as u64));
        // Shift left by multiplying with PRIME
        hash = hash.wrapping_mul(PRIME);
        // Add contribution of newest byte
        hash = hash.wrapping_add(bytes[i] as u64);

        let len = i - last_cut + 1;
        if len > MIN_CHUNK_SIZE && (hash & MASK == 0 || len > MAX_CHUNK_SIZE) {
            // println!("Created chunk from {} to {} ({} kB)", last_cut, i, (i - last_cut) as f32 / 1024.0);
            res.push(&bytes[last_cut..i - WINDOW_SIZE + 1]);
            last_cut = i - WINDOW_SIZE + 1;
        }
    }

    // Add the final chunk if necessary
    if last_cut < bytes.len() {
        // println!(
        //     "Created last chunk from {} to {} ({} kB)",
        //     last_cut,
        //     bytes.len(),
        //     (bytes.len() - last_cut) as f32 / 1024.0
        // );
        res.push(&bytes[last_cut..]);
    }

    res
}

#[cfg(test)]
mod tests {
    use super::*;
    use aes_gcm::aead::rand_core::RngCore;
    use aes_gcm::aead::OsRng;

    #[test]
    fn find_avg_chunk_size() {
        for _ in 0..10 {
            let mut data = vec![0u8; 1024 * 1024 * 10]; // 10MB
            OsRng.fill_bytes(&mut data);
            let chunks = split_in_chunks(&data);

            let mean = chunks.iter().map(|chunk| chunk.len()).sum::<usize>() as f64 / chunks.len() as f64;
            let stddev = (chunks
                .iter()
                .map(|chunk| {
                    let diff = chunk.len() as f64 - mean;
                    diff * diff
                })
                .sum::<f64>()
                / chunks.len() as f64)
                .sqrt();
            let min = chunks.iter().map(|chunk| chunk.len()).min().unwrap_or(0) as f64;

            println!(
                "Chunks: {}, mean: {} kB, std-dev: {} kB, min: {} kB",
                chunks.len(),
                mean / 1024.0,
                stddev / 1024.0,
                min / 1024.0
            );
        }
    }

    #[test]
    fn check_diff() {
        let mut data = vec![0u8; 1024 * 1024 * 10]; // 10MB
        OsRng.fill_bytes(&mut data);
        let pre_chunks = split_in_chunks(&data);
        let mut copy = data.clone();
        // A single change in middle of the data should not affect the surrounding chunks
        copy[2097968] = 1;
        let post_chunks = split_in_chunks(&copy);

        let mut diffs = 0;
        for i in 0..pre_chunks.len().min(post_chunks.len()) {
            if pre_chunks[i] != post_chunks[i] {
                diffs += 1;
            }
        }
        assert_eq!(diffs, 1);
    }

    #[test]
    fn show_chunks() {
        let mut data = vec![0; 1024];
        OsRng.fill_bytes(&mut data);
        let chunks = split_in_chunks(&data);

        for i in 0..chunks.len() {
            println!("Chunk {}: {} kB\n---\n|{}|\n---\n", i, chunks[i].len(), String::from_utf8_lossy(chunks[i]));
        }
    }
}
