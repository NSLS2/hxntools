import functools
import logging

from cycler import cycler
from boltons.iterutils import chunked

from bluesky import (plans, Msg)
import bluesky.preprocessors as bpp
import bluesky.plan_stubs as bps
from bluesky import plan_patterns

from ophyd import (Device, Component as Cpt, EpicsSignal)
from .detectors.trigger_mixins import HxnModalBase

logger = logging.getLogger(__name__)


class ScanID(Device):
    next_scan_id_proc = Cpt(EpicsSignal, 'NextScanID-Cmd.PROC')
    scan_id = Cpt(EpicsSignal, 'ScanID-I')

    def get_next_scan_id(self):
        last_id = int(self.scan_id.get())
        self.next_scan_id_proc.put(1, wait=True)

        new_id = int(self.scan_id.get())
        if last_id == new_id:
            raise RuntimeError('Scan ID unchanged. Check hxnutil IOC.')
        return new_id


dev_scan_id = ScanID('XF:03IDC-ES{Status}', name='dev_scan_id')


def get_next_scan_id():
    dev_scan_id.wait_for_connection()
    return dev_scan_id.get_next_scan_id()

def get_last_scan_id():
    dev_scan_id.wait_for_connection()
    return int(dev_scan_id.scan_id.get())



def one_nd_step(detectors, step, pos_cache):
    """
    Inner loop of an N-dimensional step scan

    This is the default function for ``per_step`` param`` in ND plans.

    Parameters
    ----------
    detectors : iterable
        devices to read
    step : dict
        mapping motors to positions in this step
    pos_cache : dict
        mapping motors to their last-set positions
    """
    def move():
        yield Msg('checkpoint')
        for motor, pos in step.items():
            if pos == pos_cache[motor]:
                # This step does not move this motor.
                continue
            yield from bps.mov(motor, pos)
            pos_cache[motor] = pos

    motors = step.keys()
    yield from move()
    yield from bps.trigger_and_read(list(detectors) + list(motors))



async def cmd_scan_setup(msg):
    detectors = msg.kwargs['detectors']
    total_points = msg.kwargs['total_points']
    count_time = msg.kwargs['count_time']

    modal_dets = [det for det in detectors
                  if isinstance(det, HxnModalBase)]

    mode = 'internal'
    for det in modal_dets:
        logger.debug('[internal trigger] Setting up detector %s', det.name)
        settings = det.mode_settings

        # Ensure count time is set prior to mode setup
        det.count_time.put(count_time)

        # start by using internal triggering
        settings.mode.put(mode)
        settings.scan_type.put('step')
        settings.total_points.put(total_points)
        det.mode_setup(mode)

    # the mode setup above should update to inform us which detectors
    # are externally triggered, in the form of the list in
    #   mode_settings.triggers
    # so update each of those to use external triggering
    triggered_dets = [det.mode_settings.triggers.get()
                      for det in modal_dets]
    triggered_dets = [triggers for triggers in triggered_dets
                      if triggers is not None]
    triggered_dets = set(sum(triggered_dets, []))

    logger.debug('These detectors will be externally triggered: %s',
                 ', '.join(det.name for det in triggered_dets))

    mode = 'external'
    for det in triggered_dets:
        logger.debug('[external trigger] Setting up detector %s', det)
        det.mode_settings.mode.put(mode)
        det.mode_setup(mode)


def setup(*, RE, debug_mode=False):
    
    async def cmd_next_scan_id(msg):
        RE.md['scan_id'] = get_next_scan_id() - 1

    async def _debug_next_scan_id(cmd):
        print('debug_next_scan_id')
        RE.md['scan_id'] = 0

    RE.register_command('hxn_scan_setup', cmd_scan_setup)

    if debug_mode:
        RE.register_command('hxn_next_scan_id', _debug_next_scan_id)
    else:
        RE.register_command('hxn_next_scan_id', cmd_next_scan_id)


def _pre_scan(dets, total_points, count_time):
    yield Msg('hxn_next_scan_id')
    yield Msg('hxn_scan_setup', detectors=dets, total_points=total_points,
              count_time=count_time)


@functools.wraps(plans.count)
def count(dets, num=1, delay=None, time=None, *, md=None):
    yield from _pre_scan(dets, total_points=num, count_time=time)
    return (yield from plans.count(dets, num=num, delay=delay, md=md))


@functools.wraps(plans.scan)
def absolute_scan(dets, motor, start, finish, intervals, time=None, *,
                  md=None):
    yield from _pre_scan(dets, total_points=intervals + 1, count_time=time)
    return (yield from plans.scan(dets, motor, start, finish, intervals + 1, md=md))


@functools.wraps(plans.relative_scan)
def relative_scan(dets, motor, start, finish, intervals, time=None, *,
                  md=None):
    yield from _pre_scan(dets, total_points=intervals + 1, count_time=time)
    return (yield from plans.relative_scan(dets, motor, start, finish,
                                           intervals+1, md=md))


@functools.wraps(plans.spiral_fermat)
def absolute_fermat(dets, x_motor, y_motor, x_start, y_start, x_range,
                    y_range, dr, factor, time=None, *, per_step=None,
                    md=None, tilt=0.0):
    cyc = plan_patterns.spiral_fermat(x_motor, y_motor, x_motor.position,
                                      y_motor.position, x_range, y_range, dr,
                                      factor, tilt=tilt)
    total_points = len(cyc)

    yield from _pre_scan(dets, total_points=total_points, count_time=time)
    return (yield from plans.spiral_fermat(dets, x_motor, y_motor, x_start,
                                           y_start, x_range, y_range, dr, factor,
                                per_step=per_step, md=md, tilt=tilt))


@functools.wraps(plans.relative_spiral_fermat)
def relative_fermat(dets, x_motor, y_motor, x_range, y_range, dr,
                    factor, time=None, *, per_step=None, md=None, tilt=0.0):
    cyc = plan_patterns.spiral_fermat(x_motor, y_motor, x_motor.position,
                                      y_motor.position, x_range, y_range, dr,
                                      factor, tilt=tilt)
    total_points = len(cyc)

    yield from _pre_scan(dets, total_points=total_points, count_time=time)
    return (yield from plans.relative_spiral_fermat(
        dets,
        x_motor, y_motor, x_range, y_range, dr, factor,
        per_step=per_step, md=md, tilt=tilt))


@functools.wraps(plans.spiral)
def absolute_spiral(dets, x_motor, y_motor, x_start, y_start, x_range,
                    y_range, dr, nth, time=None, *, per_step=None,
                    md=None, tilt=0.0):
    cyc = plan_patterns.spiral_simple(x_motor, y_motor, x_motor.position,
                                      y_motor.position, x_range, y_range, dr,
                                      nth, tilt=tilt)
    total_points = len(cyc)

    yield from _pre_scan(dets, total_points=total_points, count_time=time)
    return (yield from plans.spiral(
        dets, x_motor, y_motor, x_start, y_start,
        x_range, y_range, dr, nth, per_step=per_step,
        md=md, tilt=tilt))


@functools.wraps(plans.relative_spiral)
def relative_spiral(dets, x_motor, y_motor, x_range, y_range, dr, nth,
                    time=None, *, per_step=None, md=None, tilt=0.0):
    cyc = plan_patterns.spiral_simple(x_motor, y_motor, x_motor.position,
                                      y_motor.position, x_range, y_range, dr,
                                      nth, tilt=tilt)
    total_points = len(cyc)

    yield from _pre_scan(dets, total_points=total_points, count_time=time)
    return (yield from plans.relative_spiral(
        dets, x_motor, y_motor, x_range,
        y_range, dr, nth, per_step=per_step,
        md=md, tilt=tilt))


@functools.wraps(plans.outer_product_scan)
def absolute_mesh(dets, *args, time=None, md=None):
    if (len(args) % 4) == 1:
        if time is not None:
            raise ValueError('wrong number of positional arguments')
        args, time = args[:-1], args[-1]

    total_points = 1
    new_args = []
    add_snake = False
    for motor, start, stop, num in chunked(args, 4):
        total_points *= num
        new_args += [motor, start, stop, num]
        if add_snake:
            new_args += [False]
        add_snake = True

    yield from _pre_scan(dets, total_points=total_points, count_time=time)
    return (yield from plans.outer_product_scan(dets, *new_args, md=md,
                                                per_step=one_nd_step))


@functools.wraps(absolute_mesh)
def relative_mesh(dets, *args, time=None, md=None):
    plan = absolute_mesh(dets, *args, time=time, md=md)
    plan = bpp.relative_set_wrapper(plan)  # re-write trajectory as relative
    return (yield from bpp.reset_positions_wrapper(plan))


def _get_a2_args(*args, time=None):
    if (len(args) % 3) == 2:
        if time is not None:
            raise ValueError('Wrong number of positional arguments')
        args, time = args[:-1], args[-1]

    return args, time


@functools.wraps(plans.inner_product_scan)
def a2scan(dets, *args, time=None, md=None):
    args, time = _get_a2_args(*args, time=time)
    total_points = int(args[0])
    yield from _pre_scan(dets, total_points=total_points, count_time=time)
    return (yield from plans.inner_product_scan(dets, *args, md=md,
                                                per_step=one_nd_step))


@functools.wraps(plans.relative_inner_product_scan)
def d2scan(dets, *args, time=None, md=None):
    args, time = _get_a2_args(*args, time=time)
    total_points = int(args[0])
    yield from _pre_scan(dets, total_points=total_points, count_time=time)
    return (yield from plans.relative_inner_product_scan(dets, *args, md=md,
                                                         per_step=one_nd_step))


def scan_steps(dets, *args, time=None, per_step=None, md=None):
    '''
    Absolute scan over an arbitrary N-dimensional trajectory.

    Parameters
    ----------
    ``*args`` : {Positioner, list/sequence}
        Patterned like
            (``motor1, motor1_positions, ..., motorN, motorN_positions``)
        Where motorN_positions is a list/tuple/sequence of absolute positions
        for motorN to go to.
    time : float, optional
        applied to any detectors that have a `count_time` setting
    per_step : callable, optional
        hook for cutomizing action of inner loop (messages per step)
        See docstring of bluesky.plans.one_nd_step (the default) for
        details.
    md : dict, optional
        metadata
    '''
    if len(args) % 2 == 1:
        if time is not None:
            raise ValueError('Wrong number of positional arguments')
        args, time = args[:-1], args[-1]

    cyclers = [cycler(motor, steps) for motor, steps in chunked(args, 2)]
    cyc = sum(cyclers[1:], cyclers[0])
    total_points = len(cyc)

    if md is None:
        md = {}

    from collections import ChainMap

    md = ChainMap(md, {'plan_name': 'scan_steps'})

    plan = plans.scan_nd(dets, cyc, md=md, per_step=per_step)
    plan = plans.configure_count_time_wrapper(plan, time)

    yield from _pre_scan(dets, total_points=total_points, count_time=time)
    return (yield from plans.reset_positions_wrapper(plan))
