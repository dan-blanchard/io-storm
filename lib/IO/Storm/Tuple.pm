package IO::Storm::Tuple;
use Moose;

has 'id' => (
    is => 'rw'
);

has 'component' => (
    is => 'rw'
);

has 'stream' => (
    is => 'rw'
);

has 'task' => (
    is => 'rw'
);

has 'values' => (
    is => 'rw'
);

1;