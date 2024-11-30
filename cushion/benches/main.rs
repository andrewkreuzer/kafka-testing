use cushion::cushion::Cushion;

fn main() {
    divan::main();
}

#[divan::bench(args = [1, 2, 4, 8, 16, 32])]
fn push(n: usize) {
    let mut c = Cushion::with_capacity(n);
    c.push_back(n*2);
}


#[divan::bench(args = [1, 2, 4, 8, 16, 32])]
fn pop(n: usize) {
    let mut c: Cushion<usize> = Cushion::with_capacity(n);
    c.pop_front();
}
