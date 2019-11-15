from typing import List, Optional


def string_to_frames(s):
    frames = []
    after_split = s.split(";")
    for i in after_split:
        inter = i.split("-")
        if len(inter) == 1:
            # single frame (e.g. 5)
            frames.append(int(inter[0]))
        elif len(inter) == 2:
            inter2 = inter[1].split(",")
            # frame range (e.g. 1-10)
            if len(inter2) == 1:
                start_frame = int(inter[0])
                end_frame = int(inter[1]) + 1
                frames += list(range(start_frame, end_frame))
            # every nth frame (e.g. 10-100,5)
            elif len(inter2) == 2:
                start_frame = int(inter[0])
                end_frame = int(inter2[0]) + 1
                step = int(inter2[1])
                frames += list(range(start_frame, end_frame, step))
            else:
                raise ValueError("Wrong frame step")
        else:
            raise ValueError("Wrong frame range")
    return sorted(frames)


def get_scene_file_from_resources(resources: List[str]) -> Optional[str]:
    for resource in resources:
        if resource.lower().endswith('.blend'):
            return resource
    return None
