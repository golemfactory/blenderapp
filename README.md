# blenderapp
Containerized Blender application

# API
Requestor needs following set of directories per task:
- working directory - self explanatory
- resources directory - this is the directory when one puts resource files into before creating a task
- network resources directory - an app puts packaged resources to be uploaded to Golem's CDN there and later subtask params refer to which ones are needed
- results directory - this is where result files will be stored after an app finishes the task
- network results directory - subtasks results donwloaded from providers are meant to be put here

Provider needs only two directories:
- working directory - self explanatory
- network resources directory - files downloaded from Golem's CDN required for a given subtask computation are meant to be placed here

Those directories should be constant per task during task computation.

## Commands
All docker commands assume already mounted abovementioned directories and proper image name.

### Task creation
Put `task_params.json` file into working directory and run
`docker run create-task`
Params inside the JSON file:
```
{
    "subtasks_count": <number of subtasks as int, e.g. 20>,
    "format": <output format as string, e.g. "png">,
    "resolution": <pair [width, height], e.g. [1000, 600]>,
    "frames": <string of frames, e.g. "2-3;5">,
    "scene_file": <filepath to where in the resources is the main scene file, e.g. "cube.blend">,
    "resources": [
        "cube.blend",
    ]
}
```

### Querying for subtask
Run `docker run get-next-subtask` then inspect `subtask_id.txt` under working directory which contains a single string which is the subtask ID. Then subtask params exist in `subtask<subtask_id>.json` file under working directory. These are meant to be passed to the provider. That file also contains `resources` field which is a list of resources to be passed to the provider.

### Computing subtask
Just run `docker run compute` and that will generate `result.zip` file in the working directory which is meant to be passed back to the requestor.

### Verifying subtask result
Run `docker run verify <subtask_id>` which will generate `verdict<subtask_id>.json` file under working directory which contains a boolean indicating the result of the verification.

# Tests
When in doubt look at the `test_docker.py` file which does all of the above.