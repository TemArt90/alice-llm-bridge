"""Service entry point with sampled, credential-free relay diagnostics."""
import json
import time
import sidecar


def main():
    poll = sidecar.poll_once
    relay = sidecar.relay_job
    upload = sidecar.upload_result
    started = time.monotonic()
    count = 0

    def emit(event, **fields):
        print(json.dumps({'t': round(time.monotonic()-started, 3), 'event': event, **fields}), flush=True)

    async def traced_poll(runtime, cfg):
        nonlocal count
        t = time.monotonic()
        try:
            result = await poll(runtime, cfg)
            count += 1
            if result.get('jobs') or count % 10 == 1:
                emit('poll', seconds=round(time.monotonic()-t,3), jobs=len(result.get('jobs',[])), count=count)
            return result
        except Exception as exc:
            detail = str(exc)
            for secret in cfg.secrets.values():
                detail = detail.replace(str(secret), '[hidden]')
            emit('poll_error', seconds=round(time.monotonic()-t,3), kind=type(exc).__name__, detail=detail)
            raise

    async def traced_relay(runtime, cfg, job):
        emit('relay_begin', input_bytes=len(job.get('body','')))
        result = await relay(runtime, cfg, job)
        emit('relay_end', status=result['status'], output_bytes=len(result['body']))
        return result

    async def traced_upload(runtime, cfg, result):
        emit('upload_begin')
        ok = await upload(runtime, cfg, result)
        emit('upload_end', accepted=ok)
        return ok

    sidecar.poll_once = traced_poll
    sidecar.relay_job = traced_relay
    sidecar.upload_result = traced_upload
    try:
        sidecar.main()
    finally:
        sidecar.poll_once = poll
        sidecar.relay_job = relay
        sidecar.upload_result = upload


if __name__ == '__main__':
    main()
