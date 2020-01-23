FROM node

WORKDIR /consumer

COPY package*.json /consumer/

RUN npm install

COPY . /consumer/

EXPOSE 4001
CMD [ "npm", "start"]
