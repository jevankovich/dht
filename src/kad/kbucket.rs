use std::collections::VecDeque;
use std::net::SocketAddr;

pub const K: usize = 20;
pub const KEY_BITS: usize = 256;
pub const KEY_BYTES: usize = KEY_BITS / 8;

#[derive(Copy, Clone, PartialEq, Debug)]
pub struct NodeID {
    pub(crate) bytes: [u8; KEY_BYTES],
}

impl NodeID {
    fn leading_zeros(self) -> u32 {
        let mut ret = 0;
        for x in self.bytes.iter().map(|x| x.leading_zeros()) {
            ret += x;
            if x != 8 {
                break;
            }
        }
        ret
    }
}

impl std::ops::BitXor for NodeID {
    type Output = Self;

    fn bitxor(self, rhs: Self) -> Self {
        let mut ret = NodeID {
            bytes: [0; KEY_BYTES],
        };
        self.bytes
            .iter()
            .zip(rhs.bytes.iter())
            .map(|(a, b)| a ^ b)
            .enumerate()
            .for_each(|(i, x)| ret.bytes[i] = x);

        ret
    }
}

#[derive(Copy, Clone, Debug)]
pub struct Contact {
    pub id: NodeID,
    pub addr: SocketAddr,
}

impl PartialEq for Contact {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

struct KBucket {
    can_split: bool,
    contacts: VecDeque<Contact>,
}

pub struct KBuckets {
    indices: [u8; KEY_BITS],
    next_to_split: usize,
    k_buckets: Vec<KBucket>,
}

impl KBuckets {
    pub fn new() -> KBuckets {
        KBuckets {
            indices: [0; KEY_BITS],
            next_to_split: 0,
            k_buckets: vec![KBucket {
                can_split: true,
                contacts: VecDeque::with_capacity(K),
            }],
        }
    }

    pub fn insert(&mut self, me: NodeID, contact: Contact) -> Result<(), Contact> {
        // If the network were full, Each K-bucket 0..256 would contain 20 nodes (except for degenerate buckets).
        // Nodes from the most distant half of the network (those whose msb differs from ours, i.e., no leading zeros),
        // would be in bucket 0 and the nearest node (sharing all but the lsb) would be in bucket 255.
        // However, most networks will not have 2^256 nodes, so we can optimize for this case by starting with one K-bucket
        // and splitting it in half (so half of the nodes go in a new K-bucket and half stay in an old one).
        // This also alleviates the problem of degenerate buckets. The nearest buckets can only hold 1, 2, 4, 8, and 16 contacts.
        // The nearest four buckets can /always/ be condensed into one K-bucket. There will never be more than 253 buckets.
        //
        // This approach also optimizes the query "what are the nodes I know of closest to this key". That can be looked up
        // by sending the contents of the k-bucket containing that key.

        let bucket = (me ^ contact.id).leading_zeros();
        assert!(bucket < 256);
        let bucket = self.indices[bucket as usize] as usize;

        // Handle the case where contact is already in its bucket.
        if let Some((i, _)) = self.k_buckets[bucket]
            .contacts
            .iter()
            .enumerate()
            .find(|(_, c)| contact == **c)
        {
            let contact = self.k_buckets[bucket].contacts.remove(i).unwrap();
            self.k_buckets[bucket].contacts.push_back(contact);
            return Ok(());
        }

        // bucket is full
        if self.k_buckets[bucket].contacts.len() == K {
            if self.k_buckets[bucket].can_split {
                self.k_buckets.push(KBucket {
                    can_split: false,
                    contacts: VecDeque::with_capacity(K),
                });
                self.k_buckets.push(KBucket {
                    can_split: true,
                    contacts: VecDeque::with_capacity(K),
                });

                // Zero is the only one to ever split
                // Proof:
                //   base case: 0 is the only one in the array => only one that can split
                //   induction: When the 0th is split, it's replaced by a bucket that can (exclusively) split
                let mut old_bucket = self.k_buckets.swap_remove(0);

                // Next to split will always point at the first bucket-index that is 0.
                // Therefore, indices[next_to_split] is the contiguous half of the splittable bucket.
                self.indices[self.next_to_split] = (self.k_buckets.len() - 1) as u8;
                self.next_to_split += 1;

                for contact in old_bucket.contacts.drain(..) {
                    self.insert_unchecked(me, contact);
                }
                // Unlikely worst case, this could recur up to 253 times. Because this is a tail call,
                // it can't blow that stack.
                // If node ID's are distributed uniformly, that will almost never happen.
                return self.insert(me, contact);
            } else {
                return Err(self.k_buckets[bucket].contacts[0]); // Cannot panic
            }
        } else {
            self.k_buckets[bucket].contacts.push_back(contact);
        }
        Ok(())
    }

    fn insert_unchecked(&mut self, me: NodeID, contact: Contact) {
        let bucket = (me ^ contact.id).leading_zeros();
        let bucket = self.indices[bucket as usize] as usize;
        self.k_buckets[bucket].contacts.push_back(contact);
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn full_distant_bucket() {
        let sock = "[::]:6060".parse().unwrap();
        let me = NodeID {
            bytes: [0x0; KEY_BYTES],
        };

        let mut peer = Contact {
            id: NodeID {
                bytes: [0xFF; KEY_BYTES],
            },
            addr: sock,
        };

        let mut buckets = KBuckets::new();

        // insert 20 distinct peers
        for _ in 0..K {
            buckets.insert(me, peer).unwrap(); // Shouldn't panic
            *peer.id.bytes.last_mut().unwrap() -= 1;
        }

        *peer.id.bytes.last_mut().unwrap() = 0;
        assert_eq!(
            buckets.insert(me, peer),
            Err(Contact {
                id: NodeID {
                    bytes: [0xFF; KEY_BYTES]
                },
                addr: sock
            })
        );

        assert_eq!(buckets.k_buckets.len(), 2);
        assert_eq!(
            buckets.k_buckets[buckets.indices[0] as usize]
                .contacts
                .len(),
            K
        );
        for i in &buckets.indices[1..] {
            assert_eq!(buckets.k_buckets[*i as usize].contacts.len(), 0);
        }
    }

    #[test]
    fn full_near_bucket() {
        let sock = "[::]:6060".parse().unwrap();
        let me = NodeID {
            bytes: [0x0; KEY_BYTES],
        };

        let mut peer = Contact {
            id: NodeID {
                bytes: [0x0; KEY_BYTES],
            },
            addr: sock,
        };

        let mut buckets = KBuckets::new();

        for _ in 0..K {
            *peer.id.bytes.last_mut().unwrap() += 1;
            buckets.insert(me, peer).unwrap(); // Shouldn't panic
        }

        peer.id.bytes = [0xFF; KEY_BYTES];
        buckets.insert(me, peer).unwrap();
        assert_eq!(
            buckets.k_buckets[buckets.indices[0] as usize]
                .contacts
                .len(),
            1
        );
        assert_eq!(
            buckets.k_buckets[buckets.indices[KEY_BITS - 1] as usize]
                .contacts
                .len(),
            K
        );

        peer.id.bytes = [0; KEY_BYTES];
        *peer.id.bytes.last_mut().unwrap() = K as u8;
        buckets.insert(me, peer).unwrap(); // Should end up splitting the nearest bucket
    }
}