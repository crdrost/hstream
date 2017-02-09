// an Hstream is a version of Haskell's ListT monad, but with some ability to buffer synchronous
// lists as well

class HstreamCell {
    constructor(buff, rest) {
        this.chunk = buff;
        this.rest = rest instanceof Hstream? rest : null;
    }
}

// helper function: process an Hstream to find out how many elements at the beginning of the
// Hstream match the predicate. It is a promised int, of course.
function countPrefix(stream, predicate) {
    let k = 0, going = true;
    // count :: Hstream<Promise><x> -> Promise<Int>
    function count(str) {
        return stream.resolve().then(cell => {
            for (let item of cell.chunk) {
                if (predicate(item, k)) {
                    k++;
                } else {
                    going = false;
                    break;
                }
            }
            return going && cell.rest? count(cell.rest) : k;
        });
    }
    return count(stream);
}

class Hstream {
    // raw constructor, mostly intended to be used within this class.
    constructor(next) {
        // we ensure that this.next() only calls the above next() at most once.
        // we trust that it outputs an Items<x> object.
        let cache = null, cached = false;
        this.resolve = () => { if (!cached) { cache = fn(); cached = true } return cache; };
    }
    // convenience method to make this out of any iterable:
    static fromIterable(iterable, bufferSize=64, keepReturn=false) {
        const iter = iterable[Symbol.iterator]();
        return new Hstream(function next() {
            let {done, value} = iter.next();
            let buffer = (keepReturn || !done) ? [value] : [];
            while (!done && buffer.length < bufferSize) {
                {done, value} = iter.next();
                if (keepReturn || !done) {
                    buffer.push(value);
                }
            }
            return Promise.resolve(new HstreamCell(buffer, !done || new Hstream(next)))
        }
    }
    static empty() {
        return new Hstream(() => new HstreamCell([]));
    }
    static fromStream(stream, options) {
	// TODO: decide if I want to do these with direct `read()` functions...
        // opts: the fully defaulted version of the `options` object.
        let opts = {
            dataEvent: 'data',
            errEvent: 'error',
            endEvent: 'end',
            pauseResumeAfter: 64,
            pauseFn: 'pause',
            resumeFn: 'resume'
        };
        Object.keys(opts).forEach(key => {
            if (Object.prototype.hasOwnProperty.call(options, key)) {
                opts[key] = options[key];
            }
        });
        let buffer = [],          // a buffer that will be filled with the events seen until the promises are consumed.
            endEventSeen = false, // true if the opts.endEvent has finally been seen.
            waiting = null,       // a callback function if we try to consume a promise before data is ready.
            error = null,         // the first error witnessed through the error event.
            pauseResume = opts.pauseResumeAfter !== -1;
        stream.on(opts.dataEvent, data => {
            try {
                buffer.push(data);
                if (pauseResume && buffer.length >= opts.pauseResumeAfter) {
                    stream[opts.pauseFn]();
                }
            } catch (e) {
                // error in stream.pause? Treat as fundamental.
                if (error === null) {
                    error = e;
                }
            }
            if (waiting) {
                waiting();
            }
        });
        stream.on(opts.errEvent, e => {
            if (error === null) {
                error = e;
            }
            if (waiting) {
                waiting();
            }
        });
        stream.on(end, () => { endEventSeen = true; if (waiting) { waiting(); }});
        let next, attemptToResolve;
        // we pull out attemptToResolve because we're going to repeat ourselves putting it on the `waiting` spot above.
        next = () => new Promise((acc, rej) => attemptToResolve(true, acc, rej));
        attemptToResolve = (soft_fail, accept, reject) {
            try {
                if (buffer.length > 0) {
                    let out = buffer;
                    buffer = [];
                    if (pauseResume) {
                        stream[opts.resumeFn]();
                    }
                    accept(new HstreamCell(buffer, (!error && endEventSeen) ? null : new Hstream(next)));
                } else if (error) {
                    reject(error);
                } else if (endEventSeen) {
                    // an end event was seen while the buffer was empty, so yield the empty stream.
                    accept(new HstreamCell(buffer));
                } else if (!soft_fail) {
                    reject(new Error("Hstream internal error: The attempt to resolve this Promise failed twice; the first (soft)fail was postponed to a callback, the second should therefore not have happened."));
                } else if (waiting !== null) {
                    reject(new Error("Hstream internal error: The attempt to resolve this Promise failed softly so this promise needed to await results, but another callback was already waiting for results."));
                } else {
                    waiting = () => { waiting = null; attemptToResolve(false, accept, reject); }
                }
            } catch (e) {
                reject(e);
            }
        };
        return new Hstream(next);
    }
    // append another Hstream to this one. This does not modify the existing Hstreams; instead it
    // returns a new Hstream that is the concatenation of the two.
    append(...others) {
        function appendingTo(hstream) {
            return new Hstream(() => hstream.resolve().then(cell => {
                let rest = cell.rest;
                if (!rest && others.length > 0) {
                    rest = others.shift();
                }
                return new HstreamCell(cell.chunk, rest? appendingTo(rest) : null);
            }));
        }
        return appendingTo(this);
    }
    // process this Hstream with some stateful non-monadic walk. The arguments are the initial state,
    // a function from (state, item) to [new_state, new_items] where new_items must be an iterable,
    // and a function from state to new_items for what to do when this Hstream is done processing.
    statefully(state, on_data, on_end) {
        function* concat(gens) {
            for (let gen of gens) {
                yield* gen;
            }
        }
        function modStream(stream) { 
            return new Hstream(() => stream.resolve().then(cell => {
                let new_chunk = [];
                for (let item of cell.chunk) {
                    let [new_state, new_items] = on_data(state, item);
                    state = new_state;
                    new_chunk.push(new_items);
                }
                if (!cell.rest) {
                    new_chunk.push(on_end(state));
                }
                return new HstreamCell([...concat(new_chunk)], cell.rest? modStream(cell.rest) : null);
            }));
        }
        return modStream(this);
    }

    // line-buffering.
    lines() {
        return Hstream.prototype.statefully.call(
            this,
            {buffers: false, spillover: ''},
            (state, buf) => {
                if (!state.buffers && typeof buf !== 'string') {
                    if (!(buf instanceof Buffer)) {
                        throw new Error("Hstream.lines() should only be called on an Hstream of UTF-8 strings and buffers.");
                    }
                    state.spillover = Buffer.from(state.spillover, 'utf8');
                    state.buffers = true;
                }
                if (state.buffers && typeof buf === 'string') {
                    buf = Buffer.from(buf, 'utf8');
                }
                let catted = state.buffers? Buffer.concat(state.spillover, buf) : state.spillover + buf;
                let end = catted.lastIndexOf('\n');
                if (end === -1) {
                    state.spillover = catted;
                    return [state, []];
                }
                state.spillover = catted.slice(end + 1);
                let out = catted.slice(0, end + 1).toString('utf8').split(/\r?\n/g);
                out.pop();
                return [state, out];
            },
            state => [state.spillover.toString('utf8')]
        );
    }
    // name is stolen from Clojure, a transducer is a function which maps an A to a list of Bs,
    // possibly empty; we use this to process a stream of As and produce a stream of Bs. Unlike
    // clojure we pass a second argument, if you want it, which is just the numerical index that
    // we are in the stream.
    transduce(fn) {
        return this.statefully(0, (n, item) => [n+1, fn(item, n)], n => []);
    }
    // a map is a special case of a transduction where the output list is always of length 1, so
    // the input and output streams exist in 1-to-1 correspondence. This is just implemented as
    // a transduction on the backend.
    map(fn) {
        return this.transduce((x, n) => [fn(x, n)]);
    }
    // a filter is a special case of a transduction where the output list is always of length
    // either 0 or 1 and if its length is 1 then it is that element from the input; they are 
    // constructed by predicates which return true/false for the elements of x.
    filter(predicate) {
        return this.transduce((x, n) => predicate(x, n) ? [x] : []);
    }
    // you can take the first n elements of a Hstream to produce a new Hstream that has at most
    // that many elements...
    take(n) {
        function cropping(stream) {
            return new Hstream(() => stream.resolve().then(cell => {
                let chunk = cell.chunk.slice(0, n); 
                n -= cell.chunk.length;
                return new HstreamCell(chunk, (n > 0 && cell.rest)? cropping(cell.rest) : null);
            }));
        } 
        return cropping(this);
    }
    // or drop the first n elements instead...
    drop(n) {
        function dropping(stream) {
            return new Hstream(() => stream.resolve().then(cell => {
                if (n >= cell.chunk.length) {
                    n -= cell.chunk.length;
                    return cell.rest? dropping(rest).resolve() : new HstreamCell([]);
                }
                return new HstreamCell(cell.chunk.slice(n), cell.rest);
            }));
        }
        return dropping(this);
    }
    // finally some stateful things are a little more complex, like taking while a certain
    // function produces a true output:
    takeWhile(predicate) {
        return new Hstream(() => countPrefix(this, predicate).then(n => this.take(n).resolve()));
    }
    // or dropping while a certain function produces a true output:
    dropWhile(predicate) {
        return new Hstream(() => countPrefix(this, predicate).then(n => this.drop(n).resolve()));
    }
    pipe(writable, closeOnEnd) {
        return this.next().then(cell => {
            if (cell.done) {
                if (closeOnEnd) {
                    return new Promise((accept, reject) =>
                            writable.end(err => err? reject(err) : accept()));
                } else {
                    return Promise.resolve(undefined);
                }
            } else {
                let to_write, tmp;
                if (cell.value instanceof Buffer) {
                    to_write = cell.value;
                } else {
                    to_write = new Buffer(JSON.stringify(cell.value) + "\n", 'utf8');
                }
                tmp = writable.write(to_write);
                if (!tmp) {
                    return new Promise((accept, reject) => {
                        writable.once('drain', () => 
                                cell.iter.pipe(writable, closeOnEnd).then(accept, reject));
                    });
                } else {
                    return cell.iter.pipe(writable, closeOnEnd);
                }
            }
        });
    }
    sequence() {
        var out = [];
        function appending(jstream) {
            return jstream.next().then(x => {
                if (x.done) {
                    return Promise.resolve(out);
                } else {
                    out.push(x.value);
                    return appending(x.iter);
                }
            });
        }
        return appending(this);
    }
    sequence_() {
        function force(jstream) {
            return jstream.next().then(x => x.done ? Promise.resolve(undefined) : force(x.iter));
        }
        return force(this);
    }
}
module.exports = Hstream;

