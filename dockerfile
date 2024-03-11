FROM public.ecr.aws/lambda/python:3.10

# Copy all
COPY . ${LAMBDA_TASK_ROOT}

# Install the specified packages
RUN pip install -r requirements.txt

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "function.lambda_handler" ]
